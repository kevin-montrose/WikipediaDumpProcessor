namespace WikipediaDumpProcessor.Common
{
    public static class Constants
    {
        public const string NodeJSPath = @"C:\Program Files\nodejs\node.exe";
        public const string ConvertJSPath = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\node\convert.js";

        public const string PythonPath = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\python\Scripts\python.exe";
        public const string TopicPyPath = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\python\topic_embed.py";
        public const string AgreementPyPath = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\python\cite_agreement.py";

        public const string WikipediaIndexPath = @"C:\Users\kevin\source\wikipedia\enwiki-20241101-pages-articles-multistream-index.txt.bz2";
        public const string WikipediaDataPath = @"C:\Users\kevin\source\wikipedia\enwiki-20241101-pages-articles-multistream.xml.bz2";

        public const string DatabasePath = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\output\wikipedia.db";
        public const string CitationFilePaths = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\output";

        public const string PackedCitationIndexFilePath = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\output\citations-multistream-index.xml.bz2";
        public const string PackedCitationMultiStreamFilePath = @"C:\Users\kevin\source\repos\WikipediaDumpProcessor\output\citations-multistream.xml.bz2";
    }
}
