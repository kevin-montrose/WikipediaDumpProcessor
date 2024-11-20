namespace WikipediaDumpProcessor.Packer
{
    using ICSharpCode.SharpZipLib.BZip2;
    using static WikipediaDumpProcessor.Common.Constants;

    internal class Program
    {
        public static void Main(string[] args)
        {
            using var indexFs = File.Create(PackedCitationIndexFilePath);
            using var indexZipped = new BZip2OutputStream(indexFs);
            using var indexText = new StreamWriter(indexZipped);

            using var packedFs = File.Create(PackedCitationMultiStreamFilePath);

            var start = 0L;

            var bufArr = new byte[4096];
            var buf = bufArr.AsSpan();

            foreach (var file in Directory.EnumerateFiles(CitationFilePaths, "*.xml.bz2").OrderBy(static x => Path.GetFileNameWithoutExtension(x)))
            {
                var noExt = Path.GetFileNameWithoutExtension(file);
                noExt = Path.GetFileNameWithoutExtension(noExt);

                if (!long.TryParse(noExt, out var num))
                {
                    continue;
                }

                Console.WriteLine($"[{DateTime.UtcNow:u}] Processing: {noExt}");

                var len = 0L;

                using var toCopy = File.OpenRead(file);
                int read;
                while ((read = toCopy.Read(buf)) != 0)
                {
                    len += read;
                    packedFs.Write(buf[..read]);
                }

                indexText.WriteLine($"{num}:{start}");

                start += len;
            }
        }
    }
}
