using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc_cs
{
    internal class Program1
    {
        private record struct StationData
        (
            long Min,
            long Sum,
            long Max,
            long Count
        );

        static void Main1(string[] args)
        {
            using var stream = File.OpenRead(@"D:\repos\1brc\measurements.txt");
            Stopwatch timer = Stopwatch.StartNew();
            Dictionary<string, StationData> stations = new();
            Span<byte> buf = stackalloc byte[1024];
            int bufLen = 0;
            int bufIdx = 0;
            int totalEntries = 0;
            //for (int i = 0; i < 100_000_000; i++)
            for (int i = 0; i < 1_000_000_000; i++)
            {
                int newLineIdx = buf.Slice(bufIdx, bufLen - bufIdx).IndexOf((byte)'\n');
                if (newLineIdx != -1)
                    newLineIdx += bufIdx;
                if (newLineIdx == -1)
                {
                    if (bufIdx < bufLen)
                    {
                        buf.Slice(bufIdx, bufLen - bufIdx).CopyTo(buf);
                        bufLen = bufLen - bufIdx;
                    }
                    else
                        bufLen = 0;
                    bufLen += stream.Read(buf.Slice(bufLen));
                    if (bufLen == 0)
                        break;
                    bufIdx = 0;
                    newLineIdx = bufIdx + buf.Slice(bufIdx, bufLen - bufIdx).IndexOf((byte)'\n');
                }

                Span<byte> line = buf.Slice(bufIdx, newLineIdx - bufIdx);
                int start = 0;
                int sepIdx = line.IndexOf((byte)';');
                var name = Encoding.UTF8.GetString(line.Slice(0, sepIdx));
                start = sepIdx + 1;
                sepIdx = start + line.Slice(start).IndexOf((byte)'.');

                int parseToInt(Span<byte> span)
                {
                    int neg = 1;
                    int num = 0;
                    foreach (byte b in span)
                    {
                        if (b == '-')
                            neg = -1;
                        else if (b == '.')
                            ;
                        else
                            num = num * 10 + (b - '0');
                    }
                    return num * neg;
                }

                var temperature = parseToInt(line.Slice(start));

                ref StationData data = ref CollectionsMarshal.GetValueRefOrAddDefault(stations, name, out bool exists);
                if (!exists)
                {
                    data.Min = long.MaxValue;
                    data.Max = long.MinValue;
                }
                data.Sum += temperature;
                data.Count++;
                data.Min = Math.Min(data.Min, temperature);
                data.Max = Math.Max(data.Max, temperature);

                bufIdx = newLineIdx + 1;
                totalEntries++;
            }

            foreach (var stat in stations.OrderBy(e => e.Key))
                Console.WriteLine($"{stat.Key,20}: {stat.Value}");

            Console.WriteLine($"{timer.Elapsed}");
            Console.WriteLine($"total entries {totalEntries}");
        }
    }
}
