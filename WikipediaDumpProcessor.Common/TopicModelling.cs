namespace WikipediaDumpProcessor.Common
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Text;
    using System.Text.RegularExpressions;

    using static Constants;

    public static class TopicModelling
    {
        public static Process StartProcess()
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

        public static double[]? GetTopicVector(string text, ConcurrentBag<Process> processes)
        {
            Process? pythonProcess;
            while (!processes.TryTake(out pythonProcess))
            {
                Thread.Sleep(10);
            }

            DataReceivedEventHandler? onError = null;
            string? tempFile = null;
            try
            {
                var finalText = text.IndexOf('\n') != -1 ? Regex.Replace(text, "\r?\n", " ") : text;
                finalText = Regex.Replace(text, @"\s+", " ").Trim();

                tempFile = Path.GetTempFileName();
                File.WriteAllText(tempFile, finalText, Encoding.UTF8);

                var sb = new StringBuilder();

                var err = new StringBuilder();

                var duration = Stopwatch.StartNew();

                try
                {

                    var killed = false;
                    onError =
                        (object sender, DataReceivedEventArgs e) =>
                        {
                            lock (err)
                            {
                                err.AppendLine(e.Data);
                            }

                            if (duration.Elapsed > TimeSpan.FromSeconds(30))
                            {
                                Volatile.Write(ref killed, true);
                                pythonProcess.Kill();
                            }
                        };

                    pythonProcess.ErrorDataReceived += onError;
                    pythonProcess.BeginErrorReadLine();

                    pythonProcess.StandardInput.WriteLine(tempFile);
                    pythonProcess.StandardInput.Flush();

                    string? read;
                    while ((read = pythonProcess.StandardOutput.ReadLine()) != null)
                    {
                        sb.AppendLine(read);
                        if (read.EndsWith("]"))
                        {
                            break;
                        }
                    }

                    if (Volatile.Read(ref killed))
                    {
                        while (!pythonProcess.HasExited)
                        {
                            Thread.Sleep(10);
                        }

                        return null;
                    }

                    var toParse = sb.ToString();
                    var splitable = Regex.Replace(toParse, @"(\[|\]|\r|\n)", " ");
                    var split = splitable.Split(" ", StringSplitOptions.RemoveEmptyEntries);

                    var ret = new double[split.Length];
                    for (var i = 0; i < split.Length; i++)
                    {
                        ret[i] = double.Parse(split[i]);
                    }

                    return ret;
                }
                catch (Exception e)
                {
                    Console.WriteLine($"!!! FAILURE in Topic Vector Fetch: {e.Message} !!!");

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

                if (onError is not null && pythonProcess is not null && !pythonProcess.HasExited)
                {
                    pythonProcess.CancelErrorRead();
                    pythonProcess.ErrorDataReceived -= onError;
                }

                if (pythonProcess is not null && !pythonProcess.HasExited)
                {
                    processes.Add(pythonProcess);
                }
                else
                {
                    pythonProcess?.Dispose();

                    processes.Add(StartProcess());
                }
            }
        }
    }
}
