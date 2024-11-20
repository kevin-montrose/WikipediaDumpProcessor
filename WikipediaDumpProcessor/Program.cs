namespace WikipediaDumpProcessor
{
    using HtmlAgilityPack;
    using ICSharpCode.SharpZipLib.BZip2;
    using Microsoft.Data.Sqlite;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Diagnostics;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Xml;
    using Dapper;
    using System.Buffers.Binary;
    using WikipediaDumpProcessor.Common;

    using static WikipediaDumpProcessor.Common.Constants;

    internal class Program
    {
        public static void Main(string[] args)
        {
            var connectionStr =
                new SqliteConnectionStringBuilder()
                {
                    DataSource = DatabasePath,
                    Mode = SqliteOpenMode.ReadWriteCreate,
                    ForeignKeys = true,
                    Pooling = true,
                };

            using (var sqlConnection = new SqliteConnection(connectionStr.ConnectionString))
            {
                sqlConnection.Open();

                var topicCreateRes =
                    sqlConnection.ExecuteScalar(
                        @"CREATE TABLE IF NOT EXISTS Topics 
                          (
                            Id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (typeof(Id) = 'integer'),
                            Name TEXT CHECK (typeof(Name) = 'text'),
                            ByteStart INTEGER CHECK (typeof(ByteStart) = 'integer'),
                            ByteEnd INTEGER CHECK (typeof(ByteEnd) = 'integer'),
                            ArticleId INTEGER CHECK (typeof(ArticleId) = 'integer'),
                            TopicVector BLOB CHECK (typeof(TopicVector) = 'blob')
                          )"
                    );

                // todo: add FEVER vector here
                var citationCreateRes =
                    sqlConnection.ExecuteScalar(
                        @"CREATE TABLE IF NOT EXISTS Citations 
                          (
                            Id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (typeof(Id) = 'integer'),
                            TopicId INTEGER CHECK (typeof(TopicId) = 'integer'),
                            FileNumber INTEGER CHECK (typeof(FileNumber) = 'integer'),
                            FOREIGN KEY(TopicId) REFERENCES Topics(Id)
                          )"
                    );

                var indexCreateRes = sqlConnection.ExecuteScalar(@"CREATE UNIQUE INDEX IF NOT EXISTS Topics_Name ON Topics(Name)");
                var citeIndexCreateRes = sqlConnection.ExecuteScalar(@"CREATE INDEX IF NOT EXISTS Citations_TopicId ON Citations(TopicId)");

                var existingTopics = sqlConnection.QuerySingle<int>("SELECT COUNT(*) FROM Topics");
                var existingCitations = sqlConnection.QuerySingle<int>("SELECT COUNT(*) FROM Citations");

                Console.WriteLine($"Existing Topics: {existingTopics:N0}");
                Console.WriteLine($"Existing Citations: {existingCitations:N0}");
            }

            var parallelism = Environment.ProcessorCount;
            //var parallelism = 1;

            var nodeProcesses = new ConcurrentBag<Process>();
            var pythonProcesses = new ConcurrentBag<Process>();
            try
            {
                // read the index for ingestion
                var loadIndexTask = Task.Factory.StartNew(() => GetIndexes(), TaskCreationOptions.LongRunning);

                // start shared node & python processes
                Enumerable.Range(0, parallelism)
                    .AsParallel()
                    .WithDegreeOfParallelism(parallelism)
                    .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                    .WithMergeOptions(ParallelMergeOptions.NotBuffered)
                    .ForAll(
                        _ =>
                        {
                            pythonProcesses.Add(TopicModelling.StartProcess());
                            nodeProcesses.Add(StartNodeProcess());
                        }
                    );

                var index = loadIndexTask.GetAwaiter().GetResult();
                var totalSegments = index.Count;
                Console.WriteLine($"Total Segments: {totalSegments:N0}");

                // extract all citations, along with topic of article their in, and containing sentences and paragraphs
                var fileId = GetLastFileId(out var totalSize);
                var completedSegments = fileId;
                Console.WriteLine($"Total Files: {fileId:N0}");

                var overallStart = Stopwatch.GetTimestamp();

                index
                    .AsParallel()
                    .WithDegreeOfParallelism(parallelism)
                    .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                    .WithMergeOptions(ParallelMergeOptions.NotBuffered)
                    .ForAll(
                        articleDetailsBatch =>
                        {
                            var batchStart = Stopwatch.GetTimestamp();

                            List<(string ArticleName, long ArticleId)> subset;
                            lock (connectionStr)
                            {
                                using var sqlConnection = new SqliteConnection(connectionStr.ConnectionString);
                                sqlConnection.Open();

                                var present =
                                    sqlConnection.Query<string>(
                                        "SELECT Name FROM Topics WHERE Name in @ArticleNames",
                                        new
                                        {
                                            ArticleNames = articleDetailsBatch.Articles.Select(static x => x.ArticleName)
                                        }
                                    );

                                subset =
                                    articleDetailsBatch
                                        .Articles
                                        .Where(x => !present.Contains(x.ArticleName))
                                        .Where(x => !x.ArticleName.StartsWith("Category:"))
                                        .Where(x => !x.ArticleName.StartsWith("Template:"))
                                        .Where(x => !x.ArticleName.StartsWith("Talk:"))
                                        .Where(x => !x.ArticleName.StartsWith("Help:"))
                                        .Where(x => !x.ArticleName.StartsWith("Special:"))
                                        .Where(x => !x.ArticleName.StartsWith("Wikipedia:"))
                                        .Where(x => !x.ArticleName.StartsWith("MediaWiki:"))
                                        .Where(x => !x.ArticleName.StartsWith("Module:"))
                                        .Where(x => !x.ArticleName.StartsWith("Portal:"))
                                        .Where(x => !x.ArticleName.StartsWith("Draft:"))
                                        .Where(x => !x.ArticleName.StartsWith("List of "))
                                        .ToList();
                            }

                            List<(long ArticleId, string ArticleName, double[] TopicEmbedding, List<(string Uri, string Sentence, string Paragraph)> Citations)>? ret = null;
                            if (subset.Count != 0)
                            {
                                var subsetBatch = (articleDetailsBatch.ByteStart, articleDetailsBatch.ByteEnd, Articles: subset);

                                var articles = ReadArticles(subsetBatch);

                                foreach (var article in articles)
                                {
                                    if (!article.ArticleMarkup.StartsWith("#REDIRECT "))
                                    {
                                        var htmlAttempt = 1;

                                    tryHtmlAgain:
                                        var converted = GetArticleHtml(nodeProcesses, article.ArticleMarkup);

                                        if (converted is null)
                                        {
                                            if (htmlAttempt < 3)
                                            {
                                                htmlAttempt++;
                                                Console.WriteLine($"{article.ArticleName} => **HTML EXTRACTION FAILED - RETRYING**");
                                                goto tryHtmlAgain;
                                            }
                                            else
                                            {
                                                Console.WriteLine($"{article.ArticleName} => !!HTML EXTRACTION FAILED - SKIPPING!!");
                                            }
                                        }
                                        else
                                        {
                                            var citations = GetCitations(converted);

                                            if (citations.Count > 0)
                                            {
                                                var attempt = 1;

                                                ret ??= [];

                                                var noTemplates = Regex.Replace(converted.Text, @"< a [^>]+? title=""[^""]*? page \s does \s not \s exist [^""]*? ""> [^<]+? </ a >", " ", RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);
                                                var noTags = Regex.Replace(noTemplates, @"< \s* .+? /? \s* (\s+ [^>]*? )? >", " ", RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);
                                                var noDirectives = Regex.Replace(noTags, @"{{{ [^}]+? }}}", " ", RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);
                                                var noUrls = Regex.Replace(noDirectives, @"https? :// [^\s]+?", " ", RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);

                                            tryAgain:
                                                var topic = TopicModelling.GetTopicVector(noUrls, pythonProcesses);

                                                if (topic is null)
                                                {
                                                    if (attempt < 3)
                                                    {
                                                        attempt++;
                                                        Console.WriteLine($"{article.ArticleName} => **TOPIC MODELING FAILED - RETRYING**");
                                                        goto tryAgain;
                                                    }
                                                    else
                                                    {
                                                        Console.WriteLine($"{article.ArticleName} => !!TOPIC MODELING FAILED - SKIPPING!!");
                                                    }
                                                }
                                                else
                                                {
                                                    ret.Add((article.ArticleId, article.ArticleName, topic, citations));

                                                    //Console.WriteLine($"{article.ArticleName} => {citations.Count:N0} citations extracted{Environment.NewLine}\t[{string.Join(", ", topic.Take(5))}...");
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if (ret is null)
                            {
                                return;
                            }

                            long fileSize;
                            long totalWritten;
                            lock (connectionStr)
                            {
                                using var sqlConnection = new SqliteConnection(connectionStr.ConnectionString);
                                sqlConnection.Open();

                                using var trans = sqlConnection.BeginTransaction();

                                var fileNumber = Interlocked.Increment(ref fileId);

                                var outputPath = Path.Combine(CitationFilePaths, $"{fileNumber:0000000}.xml.bz2");
                                using (var fs = File.Create(outputPath))
                                using (var bz = new BZip2OutputStream(fs))
                                using (var writer = new StreamWriter(bz, Encoding.UTF8))
                                {
                                    writer.WriteLine(@"<?xml version=""1.0"" encoding=""UTF-8""?>");
                                    writer.WriteLine($@"<citations fileNumber=""{fileNumber}"">");

                                    foreach (var row in ret)
                                    {
                                        var encoded =
                                                row.TopicEmbedding.SelectMany(
                                                v =>
                                                {
                                                    var r = new byte[8];
                                                    BinaryPrimitives.WriteDoubleLittleEndian(r, v);
                                                    return r;
                                                }
                                            )
                                            .ToArray();

                                        var topicId =
                                            sqlConnection.QuerySingle<int>(
                                                @"INSERT INTO Topics 
                                          (Name, ByteStart, ByteEnd, ArticleId, TopicVector)
                                          VALUES
                                          (@Name, @ByteStart, @ByteEnd, @ArticleId, @TopicVector)
                                          RETURNING Id",
                                                 new
                                                 {
                                                     Name = row.ArticleName,
                                                     articleDetailsBatch.ByteStart,
                                                     articleDetailsBatch.ByteEnd,
                                                     row.ArticleId,
                                                     TopicVector = encoded,
                                                 },
                                                 transaction: trans
                                            );

                                        foreach (var cite in row.Citations)
                                        {
                                            var citeId = sqlConnection.QuerySingle<long>(
                                                @"INSERT INTO Citations
                                          (TopicId, FileNumber)
                                          VALUES
                                          (@TopicId, @FileNumber)
                                          RETURNING Id",
                                                new
                                                {
                                                    TopicId = topicId,
                                                    FileNumber = fileNumber
                                                },
                                                transaction: trans
                                            );

                                            var doc = new XmlDocument();
                                            var element = doc.CreateElement("citation");
                                            element.SetAttribute("uri", cite.Uri);
                                            element.SetAttribute("id", citeId.ToString());
                                            element.InnerText = cite.Sentence;

                                            writer.Write("    ");
                                            writer.WriteLine(element.OuterXml);
                                        }
                                    }

                                    writer.WriteLine("</citations>");
                                    writer.Flush();

                                    bz.Flush();
                                    fs.Flush();
                                }

                                fileSize = (new FileInfo(outputPath)).Length;
                                totalWritten = Interlocked.Add(ref totalSize, fileSize);

                                // only commit to DB once we've got everything on disk
                                trans.Commit();
                            }

                            var elapsedBatch = Stopwatch.GetElapsedTime(batchStart);
                            var elapsedTotal = Stopwatch.GetElapsedTime(overallStart);

                            var status = Interlocked.Increment(ref completedSegments);
                            var percDone = Math.Round(status / (double)totalSegments * 100.0, 1);

                            var batchAverage = elapsedTotal / status;
                            var remaining = batchAverage * (totalSegments - status);

                            var sizeAverage = totalWritten / status;
                            var estimatedSize = sizeAverage * totalSegments;

                            Console.WriteLine($"Complete: {percDone}% ({status:N0}/{totalSegments:N0}); took {elapsedBatch}; estimated remaining {remaining}; file size {fileSize:N0} bytes; estimated final size {estimatedSize:N0}");
                        }
                    );
            }
            finally
            {
                while (nodeProcesses.TryTake(out var proc))
                {
                    try
                    {
                        proc.Kill();
                    }
                    catch { }
                }

                while (pythonProcesses.TryTake(out var proc))
                {
                    try
                    {
                        proc.Kill();
                    }
                    catch { }
                }
            }
        }

        private static long GetLastFileId(out long totalSizeBytes)
        {
            var ret = 0L;
            totalSizeBytes = 0;

            foreach (var file in Directory.EnumerateFiles(CitationFilePaths, "*.xml.bz2"))
            {
                var num = Path.GetFileNameWithoutExtension(Path.GetFileNameWithoutExtension(file));
                var fileNum = long.Parse(num);

                ret = Math.Max(ret, fileNum);
                totalSizeBytes += (new FileInfo(file)).Length;
            }

            return ret;
        }

        private static List<(string Uri, string Sentence, string Paragraph)> GetCitations(HtmlDocument doc)
        {
            var allCitations = doc.DocumentNode.SelectNodes("//cite");

            var ret = new List<(string Uri, string Sentence, string Paragraph)>();

            if (allCitations is not null)
            {
                foreach (var cite in allCitations)
                {
                    var maybeUrl = cite.InnerText;
                    if (Uri.TryCreate(maybeUrl, UriKind.Absolute, out _))
                    {
                        var inParagraph = cite.ParentNode;
                        while (inParagraph is not null && !(inParagraph.NodeType == HtmlNodeType.Element && inParagraph.Name == "p"))
                        {
                            inParagraph = inParagraph.ParentNode;
                        }

                        if (inParagraph is not null)
                        {
                            var guid = Guid.NewGuid().ToString();

                            var paragraphRaw = inParagraph.InnerText;
                            var paragraph = paragraphRaw.Replace(maybeUrl, " ");

                            var lines = (paragraphRaw.Replace(maybeUrl, guid) + " ").Split(". ");
                            string? inLineRaw = null;
                            for (var i = 0; i < lines.Length; i++)
                            {
                                var line = lines[i].Trim();
                                if (line.StartsWith(guid))
                                {
                                    // citations are often in a trailing position, if so we look BEHIND where we found it
                                    if (i != 0)
                                    {
                                        inLineRaw = TakeFromBefore(lines, i - 1);
                                    }
                                    else
                                    {
                                        inLineRaw = lines[0];
                                    }

                                    break;
                                }
                                else if (line.Contains(guid))
                                {
                                    inLineRaw = lines[i];
                                    break;
                                }
                            }

                            if (inLineRaw is not null)
                            {
                                var inLine = inLineRaw.Replace(guid, " ");

                                ret.Add((maybeUrl, inLine, paragraph));
                            }
                        }
                    }
                }
            }

            return ret;

            static string TakeFromBefore(string[] parts, int ix)
            {
                var ret = parts[ix];

                ix--;
                while (ix >= 0 && ret.Length < 100)
                {
                    ret = parts[ix] + ". " + ret;
                    ix--;
                }

                return ret;
            }
        }

        private static Process StartNodeProcess()
        {
            var proc =
               Process.Start(
                   new ProcessStartInfo()
                   {
                       WorkingDirectory = Path.GetDirectoryName(ConvertJSPath),
                       RedirectStandardInput = true,
                       RedirectStandardOutput = true,
                       RedirectStandardError = true,
                       Arguments = ConvertJSPath,
                       FileName = NodeJSPath,
                       CreateNoWindow = true,
                   }
               );

            Debug.Assert(proc is not null);

            Console.WriteLine($"Started Html Extracting Process: Pid={proc.Id}");

            return proc;
        }

        private static HtmlDocument? GetArticleHtml(ConcurrentBag<Process> nodeProcesses, string markup)
        {
            // handle inline <ref>'s in a reasonably flexible manner
            var refRewritten =
                Regex.Replace(markup,
                    @"<ref [^>]*? > ([^<]*?) </ref>",
                    match =>
                    {
                        var noBrackets = match.Groups[1].Value.Replace("[", "").Replace("]", "").Replace("|", " ");
                        var urlIx = noBrackets.IndexOf("url=");
                        if (urlIx != -1)
                        {
                            var endUrl = noBrackets.IndexOf(" ", urlIx + 1);
                            if (endUrl == -1)
                            {
                                endUrl = noBrackets.Length;
                            }

                            var url = noBrackets[(urlIx + 4)..endUrl];
                            return "{{Cite web |url=" + url + "}}";
                        }
                        else
                        {
                            var parts = noBrackets.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                            foreach (var p in parts)
                            {
                                if (Uri.TryCreate(p, UriKind.Absolute, out _))
                                {
                                    return "{{Cite web |url=" + p + "}}";
                                }
                            }
                        }

                        return match.Value;
                    },
                    RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase
                );

            // remaining ref tags mess things up, just strip them out if we haven't already processed them
            var cleanup =
                Regex.Replace(refRewritten, @"< \s* /? \s* ref (\s+[^>]*?)? >", " ", RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);

            if (cleanup.Length == 0)
            {
                return null;
            }

            Process? nodeProc = null;
            while (!nodeProcesses.TryTake(out nodeProc))
            {
                Thread.Sleep(10);
            }

            var buffer = new List<byte>();
            buffer.EnsureCapacity(4096);

            StringBuilder err = new();

            DataReceivedEventHandler? onError = null;
            string html;
            string? tempFile = null;
            try
            {

                try
                {
                    onError =
                        (object sender, DataReceivedEventArgs e) =>
                        {
                            lock (err)
                            {
                                err.AppendLine(e.Data);
                            }
                        };

                    nodeProc.ErrorDataReceived += onError;
                    nodeProc.BeginErrorReadLine();

                    tempFile = Path.GetTempFileName();
                    File.WriteAllText(tempFile, cleanup, Encoding.UTF8);

                    byte[] toWrite = [.. Encoding.UTF8.GetBytes(tempFile), (byte)'\x07'];

                    nodeProc.StandardInput.BaseStream.Write(toWrite);
                    nodeProc.StandardInput.Flush();

                    var readBuffer = new byte[4 * 1_024];

                    int r;
                    while ((r = nodeProc.StandardOutput.BaseStream.Read(readBuffer)) != 0)
                    {
                        var take = r;

                        if (readBuffer[r - 1] == (byte)'\x07')
                        {
                            take--;
                        }

                        buffer.AddRange(readBuffer[..take]);
                        if (buffer.Count == buffer.Capacity)
                        {
                            buffer.EnsureCapacity(buffer.Capacity * 2);
                        }

                        if (take != r)
                        {
                            break;
                        }
                    }

                    html = Encoding.UTF8.GetString(buffer.ToArray());
                }
                catch (Exception e)
                {
                    Console.WriteLine($"!!! FAILURE in HTML Fetch: {e.Message} !!!");
                    return null;
                }
            }
            finally
            {
                if (tempFile is not null)
                {
                    try
                    {
                        File.Delete(tempFile);
                    }
                    catch
                    {
                        // best effort
                    }
                }

                if (onError is not null && nodeProc is not null && !nodeProc.HasExited)
                {
                    nodeProc.ErrorDataReceived -= onError;
                    nodeProc.CancelErrorRead();
                }

                if (nodeProc is not null && !nodeProc.HasExited)
                {
                    nodeProcesses.Add(nodeProc);
                }
                else
                {
                    nodeProc?.Dispose();

                    nodeProcesses.Add(StartNodeProcess());
                }
            }

            if (string.IsNullOrWhiteSpace(html))
            {
                return null;
            }

            var htmlDoc = new HtmlDocument();
            htmlDoc.LoadHtml(html);

            return htmlDoc;
        }

        private static List<(long ArticleId, string ArticleName, string ArticleMarkup)> ReadArticles((long ByteStart, long ByteEnd, List<(string ArticleName, long ArticleId)> Articles) details)
        {
            using var offset = File.OpenRead(WikipediaDataPath);
            var data = new byte[details.ByteEnd - details.ByteStart];
            offset.Position = details.ByteStart;
            offset.ReadExactly(data);

            using var inMem = new MemoryStream(data);

            using var zip = new BZip2InputStream(inMem);
            using var reader = new StreamReader(zip);

            var invalidXml = reader.ReadToEnd();

            var validXml =
                $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                $"<document>" +
                $"{invalidXml}" +
                $"</document>";

            var doc = new XmlDocument();
            doc.LoadXml(validXml);

            var ret = new List<(long ArticleId, string ArticleName, string ArticleMarkup)>();

            foreach (var art in details.Articles)
            {
                var search = $"/document/page[id = {art.ArticleId}]/revision/text";
                var page = doc.DocumentElement?.SelectSingleNode(search);
                Debug.Assert(page is not null);

                var rawText = page.InnerText;

                ret.Add((art.ArticleId, art.ArticleName, rawText));
            }

            return ret;
        }

        private static List<(long ByteStart, long ByteEnd, List<(string ArticleName, long ArticleId)> Articles)> GetIndexes()
        {
            var maxLen = (new FileInfo(WikipediaDataPath)).Length;

            var ret = new List<(long ByteStart, long ByteEnd, List<(string ArticleName, long ArticleId)> Articles)>();

            using var zip = new BZip2InputStream(File.OpenRead(WikipediaIndexPath));
            using var reader = new StreamReader(zip);

            long? prevStart = null;

            var pending = new List<(string ArticleName, long ArticleId)>();

            string? line;
            while ((line = reader.ReadLine()) is not null)
            {
                var lineSpan = line.AsSpan();

                var offsetEndIx = lineSpan.IndexOf(':');
                Debug.Assert(offsetEndIx != -1);

                var articleIdEndIx = lineSpan[(offsetEndIx + 1)..].IndexOf(':');
                Debug.Assert(articleIdEndIx != -1);
                articleIdEndIx += offsetEndIx + 1;

                var offset = long.Parse(lineSpan[..offsetEndIx]);
                var articleId = long.Parse(lineSpan[(offsetEndIx + 1)..articleIdEndIx]);
                var title = line[(articleIdEndIx + 1)..];

                if (prevStart == null)
                {
                    prevStart = offset;
                }
                else if (prevStart.Value != offset)
                {
                    ret.Add((prevStart.Value, offset, pending.ToList()));

                    pending.Clear();
                    prevStart = offset;
                }
                else
                {
                    pending.Add((title, articleId));
                }
            }

            prevStart ??= 0;

            ret.Add((prevStart.Value, maxLen, pending.ToList()));

            return ret;
        }
    }
}