using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc_cs;

internal class Program2
{
    struct StationEntry
    {
        public StationEntry() { }

        public int NameHash;
        public bool Collided;
        public int NameLength;
        public NameSpan Name;
        public string? NameStr;

        public long Count;
        public long Sum;
        public long Min = long.MaxValue;
        public long Max = long.MinValue;

        public double Avg; // Stored normally for verifying

        [InlineArray(100)]
        public struct NameSpan
        {
            private byte _element0;
        }
    }

    //const int TableSize = 2048;
    const int TableSize = 16384;
    //const int TableSize = 131072;

    //const string path = @"D:\repos\1brc\measurements_100.txt";
    //const string resultPath = @"D:\repos\1brc\result_100.txt";
    //const long correctMeasurmentCount = 100;

    const string path = @"D:\repos\1brc\measurements.txt";
    const string resultPath = @"D:\repos\1brc\result_1b.txt";
    const long correctMeasurmentCount = 1_000_000_000;

    static unsafe void Main(string[] args)
    {
        bool verify = args.Contains("-verify");
        bool noResult = args.Contains("-noresult");
        bool noPrint = args.Contains("-noprint");

        Stopwatch timer = Stopwatch.StartNew();
        Stopwatch timerUnmap = Stopwatch.StartNew();

        List<Task> tasks = new();
        Dictionary<string, StationEntry> result = new();
        long totalEntries = 0;
        List<int> searchItereations = new();

        using (var file = MemoryMappedFile.CreateFromFile(path))
        {
            using var assessor = file.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            byte* filePtr = null;
            try
            {
                assessor.SafeMemoryMappedViewHandle.AcquirePointer(ref filePtr);
                long fileSize = assessor.Capacity;
                int procs = Environment.ProcessorCount;
                int partitionSize = (int)Math.Ceiling((double)fileSize / procs);
                bool first = true;

                if (!noPrint)
                    Console.WriteLine($"file size {fileSize:n0} part size {partitionSize:n0} procs {procs}");
                for (long fileIdx = 0; fileIdx < fileSize; fileIdx += partitionSize)
                {
                    int len = (int)Math.Min(fileSize - fileIdx, partitionSize);
                    long partitionStart = fileIdx;
                    bool isFirst = first || filePtr[fileIdx - 1] == (byte)'\n';
                    tasks.Add(Task.Run(() =>
                    {
                        Span<byte> buf = new Span<byte>(filePtr + partitionStart, (int)Math.Min(fileSize - partitionStart, int.MaxValue));
                        StationEntry[] table = new StationEntry[TableSize];
                        int maxSearchIterations = 0;

                        int entries = ParsePartition(buf, len, table, isFirst, ref maxSearchIterations);

                        Interlocked.Add(ref totalEntries, entries);
                        lock (result)
                            AccumulateResult(result, table);
                        lock (searchItereations)
                            searchItereations.Add(maxSearchIterations);
                    }));
                    first = false;
                }

                Task.WhenAll(tasks).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            finally
            {
                assessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
            timerUnmap.Restart();
        }
        timerUnmap.Stop();

        foreach (var (name, e) in result.OrderBy(e => e.Key))
        {
            var e2 = e;
            e2.Avg = Math.Round((double)e.Sum / 10 / e.Count * 10) / 10;
            if (!noResult)
                Console.WriteLine($"{name,25} min {e.Min / 10.0,5:n1} avg {e2.Avg,5:n1} max {e.Max / 10.0,5:n1} cnt {e.Count,10}");
            result[name] = e2;
        }

        timer.Stop();

        if (verify)
            Verify(result, resultPath, correctMeasurmentCount);

        if (!noPrint)
        {
            Console.WriteLine($"{timer.Elapsed - timerUnmap.Elapsed}");
            Console.WriteLine($"totalEntries {totalEntries:n0}");
            Console.WriteLine($"station count {result.Count:n0}");
            Console.WriteLine($"maxSearchIterations {searchItereations.Max():n0}");
            Console.WriteLine($"unmap took {timerUnmap.Elapsed}");
        }
    }

    static void AccumulateResult(Dictionary<string, StationEntry> result, StationEntry[] table)
    {
        IEnumerable<int> indexes = Enumerable
            .Range(0, TableSize)
            .Where(i => table[i].NameLength > 0);
        foreach (int i in indexes)
        {
            string name = Encoding.UTF8.GetString(((Span<byte>)table[i].Name).Slice(0, table[i].NameLength));
            ref StationEntry entry = ref CollectionsMarshal.GetValueRefOrAddDefault(result, name, out bool exists);
            if (!exists)
                entry = new();
            entry.Sum += table[i].Sum;
            entry.Count += table[i].Count;
            entry.Min = Math.Min(entry.Min, table[i].Min);
            entry.Max = Math.Max(entry.Max, table[i].Max);
        }
    }

    static int ParsePartition(
        Span<byte> span,
        int length,
        StationEntry[] table,
        bool isFirst,
        ref int maxSearchIterations)
    {
        int totalEntries = 0;
        int idx = 0;
        if (!isFirst)
            idx = span.IndexOf((byte)'\n') + 1;

        while (idx < length)
        {
            int newLineIdx = span.Slice(idx).IndexOf((byte)'\n');
            if (newLineIdx == -1)
                break;
            newLineIdx += idx;

            Span<byte> line = span.Slice(idx, newLineIdx - idx);

            int start = 0;
            int sepIdx = line.IndexOf((byte)';');

            Span<byte> name = line.Slice(0, sepIdx);
            start = sepIdx + 1;

            int temperature = ParseToInt(line.Slice(start));

            ref StationEntry entry = ref FindEntry(table, name,
                ref maxSearchIterations);
            entry.Sum += temperature;
            entry.Count++;
            entry.Min = Math.Min(entry.Min, temperature);
            entry.Max = Math.Max(entry.Max, temperature);

            idx = newLineIdx + 1;
            totalEntries++;
        }

        return totalEntries;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int DoHash(Span<byte> name)
    {
        //HashCode hasher = new();
        //hasher.AddBytes(name);
        //return hasher.ToHashCode();
        int hash = 0;
        int i;
        for (i = 0; i + 4 <= name.Length; i += 4)
            hash += Unsafe.As<byte, int>(ref name[i]);
        for (i = name.Length / 4 * 4; i < name.Length; i++)
            hash += name[i];
        return hash;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ref StationEntry FindEntry(
        StationEntry[] table,
        Span<byte> name,
        ref int maxSearchIterations)
    {
        int hash = DoHash(name);
        int i = hash & (TableSize - 1);
        ref StationEntry entry = ref table[i];
        int searchIterations = 0;
        bool collided = false;
        while (true)
        {
            entry = ref table[i];
            if (entry.NameLength == 0)
                break;
            if (entry.NameHash == hash)
            {
                if (!entry.Collided ||
                    ((Span<byte>)entry.Name).Slice(0, entry.NameLength)
                        .SequenceEqual(name))
                {
                    break;
                }
                entry.Collided = true;
                collided = true;
            }
            searchIterations++;
            i = (i + 1) & (TableSize - 1);
        }
        if (entry.NameLength == 0)
        {
            entry = new();
            entry.NameHash = hash;
            entry.NameLength = name.Length;
            entry.Collided = collided;
            name.CopyTo(entry.Name);
        }
        maxSearchIterations = Math.Max(maxSearchIterations, searchIterations);
        return ref entry;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int ParseToInt(Span<byte> span)
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

    static void Verify(Dictionary<string, StationEntry> result, string resultPath, long correctMeasurementCount)
    {
        Dictionary<string, StationEntry> official = File.ReadAllLines(resultPath)
            .Select(line =>
            {
                string[] name = line.Split('=', 2);
                string[] data = name[^1].Split('/', 3);
                StationEntry entry = new();
                entry.NameStr = name[0];
                entry.Min = (long)(double.Parse(data[0]) * 10);
                entry.Avg = double.Parse(data[1]);
                entry.Max = (long)(double.Parse(data[2]) * 10);
                return entry;
            })
            .ToDictionary(e => e.NameStr!, e => e);
        if (official.Count != result.Count)
            Console.WriteLine($"! station count should be {official.Count} but is {result.Count}");
        long measurementCount = result.Sum(e => e.Value.Count);
        if (measurementCount != correctMeasurementCount)
            Console.WriteLine($"! measurement count should be {correctMeasurementCount} but is {measurementCount}");
        List<string> missedStations = official.Keys.Except(result.Keys).Order().ToList();
        foreach (string s in missedStations)
            Console.WriteLine($"! missing station \"{s}\"");
        List<string> extraStations = result.Keys.Except(official.Keys).Order().ToList();
        foreach (string s in extraStations)
            Console.WriteLine($"! extra station ??? \"{s}\"");
        var sortedOfficial = official.Keys.Except(missedStations).Order().ToList();
        foreach (string name in sortedOfficial)
        {
            StationEntry a = official[name];
            StationEntry b = result[name];
            if (!(a.Min == b.Min && a.Avg == b.Avg && a.Max == b.Max))
            {
                string sa = $"{a.Min / 10.0:n1}/{a.Avg:n1}/{a.Max / 10.0:n1}";
                string sb = $"{b.Min / 10.0:n1}/{b.Avg:n1}/{b.Max / 10.0:n1}";
                Console.WriteLine($"! \"{name}\" should be {sa} but is {sb}");
            }
        }
    }
}
