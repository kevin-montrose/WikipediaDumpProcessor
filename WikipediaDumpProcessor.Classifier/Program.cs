namespace WikipediaDumpProcessor.Classifier
{
    using ICSharpCode.SharpZipLib.BZip2;
    using Microsoft.Data.Sqlite;
    using System.Text;
    using Dapper;

    using static WikipediaDumpProcessor.Common.Constants;
    using System.Buffers.Binary;
    using System.Diagnostics;
    using System.Collections.Concurrent;
    using WikipediaDumpProcessor.Common;
    using System.Runtime.Intrinsics;
    using System.Xml;
    using System.Text.RegularExpressions;

    internal class Program
    {
        private struct IdVectorPair
        {
            public long Id { get; set; }
            public byte[] TopicVector { get; set; }
        }

        private struct Citation
        {
            public long Id { get; set; }
            public int FileNumber { get; set; }
        }

        public static void Main(string[] args)
        {
            const int TopicMatches = 5;

            var connectionStr =
                new SqliteConnectionStringBuilder()
                {
                    DataSource = DatabasePath,
                    Mode = SqliteOpenMode.ReadOnly,
                    ForeignKeys = true,
                    Pooling = true,
                };

            var topicProcess = new ConcurrentBag<Process>() { StartTopicPythonProcess() };
            //var agreementProcess = new ConcurrentBag<Process>() { StartAgreementPythonProcess() };
            var agreementProcess = new ConcurrentBag<Process>() { default! };
            try
            {
                var map = LoadCitationMap();
                var allVectors = LoadVectors();

                while (true)
                {
                    var readText = ReadTextToAnalyze();

                    var vector = TopicModelling.GetTopicVector(readText, topicProcess);
                    if (vector == null)
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:u}] !!Could Not Classify!!");
                        Console.WriteLine();
                        continue;
                    }

                    var getBestMatches = GetTopMatches(vector, allVectors, TopicMatches);

                    if (getBestMatches.Length == 0)
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:u}] No good topic matches found");
                        Console.WriteLine();
                        continue;
                    }

                    Console.WriteLine();
                    Console.WriteLine($"[{DateTime.UtcNow:u}] Checking citations");

                    var citations = new List<(string ArticleName, string Sentence, string Url)>();

                    using (var sqlConnection = new SqliteConnection(connectionStr.ConnectionString))
                    {
                        sqlConnection.Open();

                        foreach (var match in getBestMatches)
                        {
                            var article = sqlConnection.QuerySingle<string>("SELECT Name FROM Topics WHERE Id = @Id", new { Id = match });
                            var citationRecords = sqlConnection.Query<Citation>("SELECT Id, FileNumber FROM Citations WHERE TopicId = @Id", new { Id = match }).ToList();

                            Console.WriteLine($"[{DateTime.UtcNow:u}] \t- {article}: {citationRecords.Count} citations");
                            var cites = LoadCitations(map, citationRecords);

                            foreach (var cite in cites)
                            {
                                Console.WriteLine($"[{DateTime.UtcNow:u}] \t\t* {cite.Sentence}");
                                Console.WriteLine($"[{DateTime.UtcNow:u}] \t\t* {cite.Url}");
                                citations.Add((article, cite.Sentence, cite.Url));
                            }
                        }
                    }

                    var lines = SplitIntoLines(readText);
                    var agreementScores = CalculateSupport(lines, [.. citations], agreementProcess);
                    foreach (var line in agreementScores)
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:u}] \t- {line.Line}");
                        foreach (var score in line.Support)
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:u}] \t\t* {score.Sentence}");
                            Console.WriteLine($"[{DateTime.UtcNow:u}] \t\t\t+ Contra={score.ContraScore}");
                            Console.WriteLine($"[{DateTime.UtcNow:u}] \t\t\t+ Entail={score.EntailScore}");
                        }
                    }
                }
            }
            finally
            {
                while (topicProcess.TryTake(out var proc))
                {
                    proc.Kill();
                    proc.Dispose();
                }
            }
        }

        private static string[] SplitIntoLines(string rawInput)
        => [rawInput];  // todo: actually implement!

        private static (string Line, (string Sentence, string Url, double ContraScore, double EntailScore)[] Support)[] CalculateSupport(string[] lines, (string ArticleName, string Sentence, string Url)[] citations, ConcurrentBag<Process> procs)
        {
            Process? agreeProc;
            while (!procs.TryTake(out agreeProc))
            {
                Thread.Sleep(1_000);
            }

            var tempFile = Path.GetTempFileName();
            try
            {
                using (var writer = File.CreateText(tempFile))
                {
                    foreach (var hypothesis in lines)
                    {
                        var cleanHypothesis = Regex.Replace(hypothesis.Replace("\r\n", " ").Replace("\n", " "), @"https?://[^\s]+", " ").Replace(":", " ").Trim();

                        var written = new HashSet<string>();

                        foreach (var (name, premise, _) in citations)
                        {
                            var cleanPremise = Regex.Replace(premise.Replace("\r\n", " ").Replace("\n", " "), @"https?://[^\s]+", " ").Replace(":", " ").Trim();
                            if (!written.Add(cleanPremise))
                            {
                                continue;
                            }

                            writer.WriteLine(cleanPremise);
                            writer.WriteLine(cleanHypothesis);
                        }
                    }

                    writer.Flush();
                }

                agreeProc.StandardInput.WriteLine(tempFile);

                var ret = new List<(string Line, (string Sentence, string Url, double ContraScore, double EntailScore)[])>();

                for (var lineIx = 0; lineIx < lines.Length; lineIx++)
                {
                    var line = lines[lineIx];

                    var scores = new List<(string Sentence, string Url, double ContraScore, double EntailScore)>();

                    for (var citeIx = 0; citeIx < citations.Length; citeIx++)
                    {
                        var score = agreeProc.StandardOutput.ReadLine();
                        if (score is null)
                        {
                            throw new Exception("Couldn't process");
                        }

                        var parts = score.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length != 2)
                        {
                            throw new Exception("Bad response!");
                        }

                        var contraTxt = parts[0].Trim("[ ");
                        var entailTxt = parts[1].Trim("] ");
                        var contra = double.Parse(contraTxt);
                        var entail = double.Parse(entailTxt);

                        var cite = citations[citeIx];

                        scores.Add((cite.Sentence, cite.Url, contra, entail));
                    }

                    ret.Add((line, scores.ToArray()));
                }

                return [.. ret];
            }
            finally
            {
                try
                {
                    File.Delete(tempFile);
                }
                catch
                {
                    // best effort
                }

                if (agreeProc is null || agreeProc.HasExited)
                {
                    agreeProc?.Dispose();
                    var newAgreeProc = StartAgreementPythonProcess();

                    procs.Add(newAgreeProc);
                }
                else
                {
                    procs.Add(agreeProc);
                }
            }
        }

        private static IEnumerable<(string Sentence, string Url)> LoadCitations(Dictionary<long, (long Offset, long Length)> citeFileMap, List<Citation> rawCitations)
        {
            var files = rawCitations.Select(static r => r.FileNumber).Distinct().ToList();
            var ids = rawCitations.Select(static r => r.Id).ToHashSet();

            foreach (var file in files)
            {
                var (offset, length) = citeFileMap[file];

                using var fs = File.OpenRead(PackedCitationMultiStreamFilePath);
                fs.Position = offset;

                var into = new byte[length];
                fs.ReadExactly(into);

                using var bz = new BZip2InputStream(new MemoryStream(into));
                using var reader = new StreamReader(bz);

                var text = reader.ReadToEnd();
                var doc = new XmlDocument();
                doc.LoadXml(text);

                var cites = doc.SelectNodes("//citation");
                if (cites is null)
                {
                    throw new InvalidOperationException("There should be data here...");
                }

                foreach (XmlElement cite in cites)
                {
                    var id = long.Parse(cite.GetAttribute("id"));

                    if (ids.Contains(id))
                    {
                        var uri = cite.GetAttribute("uri");
                        var sentence = cite.InnerText;

                        yield return (sentence, uri);
                    }
                }
            }
        }

        private static long[] GetTopMatches(double[] vector, float[][][] allVectors, int take)
        {
            //var degree = 1;
            var degree = Environment.ProcessorCount;

            var reducePrecision = vector.Select(static x => (float)x).ToArray();

            var numFloatsInVec = reducePrecision.Length;

            if ((reducePrecision.Length % Vector256<float>.Count) != 0)
            {
                throw new NotImplementedException($"Assumes topic vector is multiple of {Vector256<float>.Count}");
            }

            // hard assumption that ||reducePrecision|| == 1
            var expected = 0f;
            for (var i = 0; i < reducePrecision.Length; i++)
            {
                expected += reducePrecision[i] * reducePrecision[i];
            }

            expected = MathF.Sqrt(expected);

            if (Math.Abs(expected - 1f) > 0.01f)
            {
                throw new InvalidOperationException("Target outside accepted range");
            }

            var results =
                allVectors
                    .AsParallel()
                    .WithDegreeOfParallelism(degree)
                    .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                    .WithMergeOptions(ParallelMergeOptions.NotBuffered)
                    .SelectMany(
                        static (slab, slabIndex) =>
                        {
                            var start = slabIndex * slab.Length;
                            return slab.Select((vec, ix) => (Index: start + ix, Vector: vec)).Where(static x => x.Vector is not null);
                        }
                    )
                    .Select(
                        vecT =>
                        {
                            var (index, vector) = vecT;

                            // hard assumption that ||vector|| == 1

                            var dot = 0f;

                            for (var vI = 0; vI < reducePrecision.Length; vI += Vector256<float>.Count)
                            {
                                var target = Vector256.LoadUnsafe(ref reducePrecision[vI]);
                                var doc = Vector256.LoadUnsafe(ref vector[vI]);

                                dot += Vector256.Dot(target, doc);
                            }

                            return (Index: index, CosineSimilarity: dot);
                        }
                    )
                    .OrderByDescending(static vecT => vecT.CosineSimilarity)
                    .Take(take)
                    .ToList();

            return results.Where(static x => x.CosineSimilarity > 0).Select(static x => (long)x.Index).ToArray();
        }

        private static Process StartTopicPythonProcess()
        {
            var proc =
               Process.Start(
                   new ProcessStartInfo()
                   {
                       WorkingDirectory = Path.GetDirectoryName(TopicPyPath),
                       RedirectStandardInput = true,
                       RedirectStandardOutput = true,
                       RedirectStandardError = true,
                       Arguments = "-i " + TopicPyPath,
                       FileName = PythonPath,
                       CreateNoWindow = true,
                       UseShellExecute = false,
                   }
               );

            Debug.Assert(proc is not null);

            var readyStr = proc.StandardOutput.ReadLine();

            if (readyStr != "Ready")
            {
                throw new InvalidOperationException();
            }

            Console.WriteLine($"Started Topic Modeling Process: Pid={proc.Id}");

            return proc;
        }

        private static Process StartAgreementPythonProcess()
        {
            var proc =
               Process.Start(
                   new ProcessStartInfo()
                   {
                       WorkingDirectory = Path.GetDirectoryName(AgreementPyPath),
                       RedirectStandardInput = true,
                       RedirectStandardOutput = true,
                       RedirectStandardError = true,
                       Arguments = "-i " + AgreementPyPath,
                       FileName = PythonPath,
                       CreateNoWindow = true,
                       UseShellExecute = false,
                   }
               );

            Debug.Assert(proc is not null);

            var readyStr = proc.StandardOutput.ReadLine();

            if (readyStr != "Ready")
            {
                throw new InvalidOperationException();
            }

            Console.WriteLine($"Started Agreement Modeling Process: Pid={proc.Id}");

            return proc;
        }

        private static float[][][] LoadVectors()
        {
            const int SlabSize = 1_024 * 1_024;

            var connectionStr =
                new SqliteConnectionStringBuilder()
                {
                    DataSource = DatabasePath,
                    Mode = SqliteOpenMode.ReadOnly,
                    ForeignKeys = true,
                    Pooling = true,
                };

            using (var sqlConnection = new SqliteConnection(connectionStr.ConnectionString))
            {
                sqlConnection.Open();

                var total = sqlConnection.QuerySingle<long>("SELECT COUNT(*) FROM Topics");

                Console.Write($"[{DateTime.UtcNow:u}] Loading Vector Space ({total:N0}): ");

                var ret = new List<float[][]>();
                var curStart = 0;
                var curSlice = new float[SlabSize][];

                foreach (var row in sqlConnection.Query<IdVectorPair>("SELECT Id, TopicVector FROM Topics ORDER BY Id ASC", buffered: false))
                {
                tryAgain:
                    var offset = row.Id - curStart;

                    if (offset >= curSlice.Length)
                    {
                        ret.Add(curSlice);
                        curSlice = new float[SlabSize][];

                        curStart += SlabSize;

                        goto tryAgain;
                    }

                    var topicVector = row.TopicVector.AsSpan();
                    var decoded = new float[topicVector.Length / 8];
                    for (var i = 0; i < row.TopicVector.Length; i += 8)
                    {
                        var val = BinaryPrimitives.ReadDoubleLittleEndian(topicVector.Slice(i, 8));
                        decoded[i / 8] = (float)val;
                    }

                    var mag = 0f;
                    for (var i = 0; i < decoded.Length; i++)
                    {
                        mag += decoded[i] * decoded[i];
                    }

                    mag = MathF.Sqrt(mag);

                    if (Math.Abs(mag - 1f) > 0.01f)
                    {
                        throw new InvalidOperationException("Target outside accepted range");
                    }

                    curSlice[offset] = decoded;

                    if ((offset % 10_000) == 0)
                    {
                        Console.Write("#");
                    }
                }

                ret.AddRange(curSlice);

                Console.WriteLine();
                Console.WriteLine($"[{DateTime.UtcNow:u}] Vector Space Loaded");

                return ret.ToArray();
            }
        }

        private static string ReadTextToAnalyze()
        {
            Console.WriteLine("Text to analyze (submit with empty line):");
            var sb = new StringBuilder();

            string? line;
            while ((line = Console.ReadLine()) is not null)
            {
                if (string.IsNullOrEmpty(line))
                {
                    break;
                }

                sb.AppendLine(line);
            }

            return sb.ToString();
        }

        private static Dictionary<long, (long Offset, long Length)> LoadCitationMap()
        {
            var map = new Dictionary<long, (long Offset, long Length)>();

            using var fs = File.OpenRead(PackedCitationIndexFilePath);
            using var bz = new BZip2InputStream(fs);
            using var text = new StreamReader(bz);

            var lastNum = -1L;
            var lastOffset = -1L;

            string? line;
            while ((line = text.ReadLine()) != null)
            {
                var part = line.Split(':');
                var num = long.Parse(part[0]);
                var offset = long.Parse(part[1]);

                if (lastNum != -1)
                {
                    var length = offset - lastOffset;

                    map.Add(lastNum, (lastOffset, length));
                }

                lastNum = num;
                lastOffset = offset;
            }

            if (lastOffset != -1)
            {
                var fileLen = new FileInfo(PackedCitationIndexFilePath).Length;
                map.Add(lastNum, (lastOffset, fileLen - lastOffset));
            }

            return map;
        }
    }
}
