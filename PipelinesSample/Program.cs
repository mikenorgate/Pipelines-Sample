using System;
using System.Diagnostics;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;

namespace PipelinesSample
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var client = new HttpClient();
            using (var httpStream = await client.GetStreamAsync("https://datasets.imdbws.com/title.basics.tsv.gz"))
            await using (var gzipStream = new GZipStream(httpStream, CompressionMode.Decompress))
            {
                var sw = Stopwatch.StartNew();
                await using (var parser = new DelimiterSeparatedValueParser(gzipStream, '\t'))
                {
                    while (await parser.MoveNextRecord())
                    {
                        while (await parser.MoveNextColumn())
                        {
                            if (parser.CurrentColumn == "primaryTitle")
                                break;
                        }
                    }
                }

                sw.Stop();
                Console.WriteLine(sw.Elapsed);
            }
        }
    }
}