//#define USE_BUILTIN_MAP
//#define USE_BUILTIN_HASH

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc_cs;

internal class Program2
{
    // A non owning unsafe ReadOnlySpan<byte>
    unsafe readonly struct Utf8String : IEquatable<Utf8String>, IComparable<Utf8String>
    {
        public readonly int Hash;
        public readonly int Length;
        public readonly byte* Ptr;

        public ReadOnlySpan<byte> Span => new(Ptr, Length);

        public Utf8String(int hash, ReadOnlySpan<byte> span)
        {
            Hash = hash;
            Length = span.Length;
            fixed (byte* ptr = span)
                Ptr = ptr;
        }

        public override int GetHashCode() => Hash;

        public bool Equals(Utf8String other) => Hash == other.Hash && Span.SequenceEqual(other.Span);

        public override bool Equals([NotNullWhen(true)] object? obj) => throw new NotImplementedException();

        public override string ToString() => Encoding.UTF8.GetString(Span);

        public int CompareTo(Utf8String other) => Span.SequenceCompareTo(other.Span);

        public static bool operator ==(Utf8String a, Utf8String b) => a.Equals(b);

        public static bool operator !=(Utf8String a, Utf8String b) => !a.Equals(b);
    }

    struct StationEntry
    {
        public StationEntry(Utf8String name)
        {
            Name = name;
        }

        public readonly Utf8String Name;
        public StationData Data;
    }

    struct StationData
    {
        public StationData() { }

        public long Count;
        public long Sum;
        public long Min = long.MaxValue;
        public long Max = long.MinValue;

        public double Avg; // Stored normally for verifying
    }

    //const int TableSize = 2048;
    const int TableSize = 16384;
    //const int TableSize = 16384 << 2;

    const string path = @"measurements.txt";
    const string resultPath = @"result_1b.txt";
    const long correctMeasurementCount = 1_000_000_000;

    //const string path = @"measurements3.txt";
    //const string resultPath = @"result_10k_1b.txt";
    //const long correctMeasurementCount = 1_000_000_000;

    static unsafe void Main(string[] args)
    {
        bool verify = args.Contains("-verify");
        bool noResult = args.Contains("-noresult");
        bool noPrint = args.Contains("-noprint");

        Stopwatch timer = Stopwatch.StartNew();
        Stopwatch timerUnmap = Stopwatch.StartNew();

        using (var file = MemoryMappedFile.CreateFromFile(path))
        {
            Dictionary<Utf8String, StationData> result = new();
            long totalEntries = 0;
            List<int> searchIterations = new();

            using var assessor = file.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            byte* filePtr = null;
            try
            {
                assessor.SafeMemoryMappedViewHandle.AcquirePointer(ref filePtr);
                if (filePtr is null)
                    throw new Exception($"filePtr is null");
                long fileSize = assessor.Capacity;
                int chunkSize = 150_000_000;

                int procs = Environment.ProcessorCount;
                //int procs = 8;

                if (!noPrint)
                    Console.WriteLine($"File size {fileSize:n0} chunk size {chunkSize:n0} procs {procs}");

                long cursor = 0;

                List<Task> tasks = new();
                for (int i = 0; i < procs; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        WorkerLoop(filePtr, fileSize, ref cursor, ref totalEntries, chunkSize, result, searchIterations);
                    }));
                }

                Task.WhenAll(tasks).ConfigureAwait(false).GetAwaiter().GetResult();

                foreach (var (name, e) in result.OrderBy(e => e.Key))
                {
                    var e2 = e;
                    e2.Avg = Math.Round((double)e.Sum / 10 / e.Count * 10) / 10;
                    if (!noResult)
                        Console.WriteLine($"{name,25} ({name.Hash & (TableSize - 1):X8}) " +
                            $"min {e.Min / 10.0,5:n1} avg {e2.Avg,5:n1} max {e.Max / 10.0,5:n1} cnt {e.Count,10}");
                    result[name] = e2;
                }

                timer.Stop();

                if (verify && resultPath != "" && File.Exists(resultPath))
                {
                    Dictionary<string, StationData> resultWithChars = result
                        .ToDictionary(e => e.Key.ToString(), e => e.Value);
                    Verify(resultWithChars, resultPath, correctMeasurementCount);
                }

                if (!noPrint)
                {
                    Console.WriteLine($"{timer.Elapsed}");
                    Console.WriteLine($"totalEntries {totalEntries:n0}");
                    Console.WriteLine($"Station count {result.Count:n0}");
                    Console.WriteLine($"maxSearchIterations {searchIterations.Max():n0}");
                }
            }
            finally
            {
                assessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
            timerUnmap.Restart();
        }
        Console.WriteLine($"Unmap took {timerUnmap.Elapsed}");
    }

    static void AccumulateResult(Dictionary<Utf8String, StationData> result, StationEntry[] table)
    {
        IEnumerable<int> indexes = Enumerable
            .Range(0, TableSize)
            .Where(i => table[i].Name.Length > 0);
        Console.WriteLine($"Accumulating {indexes.Count()}");
        foreach (int i in indexes)
        {
            ref StationEntry entry = ref table[i];
            ref StationData resultData = ref CollectionsMarshal.GetValueRefOrAddDefault(result, entry.Name, out bool exists);
            if (!exists)
                resultData = new();
            resultData.Sum += entry.Data.Sum;
            resultData.Count += entry.Data.Count;
            resultData.Min = Math.Min(resultData.Min, entry.Data.Min);
            resultData.Max = Math.Max(resultData.Max, entry.Data.Max);
        }
    }

    static void AccumulateResult(Dictionary<Utf8String, StationData> result, Dictionary<Utf8String, StationData> table)
    {
        Console.WriteLine($"Accumulating {table.Count}");
        foreach (var (name, data) in table)
        {
            ref StationData resultData = ref CollectionsMarshal.GetValueRefOrAddDefault(result, name, out bool exists);
            if (!exists)
                resultData = new();
            resultData.Sum += data.Sum;
            resultData.Count += data.Count;
            resultData.Min = Math.Min(resultData.Min, data.Min);
            resultData.Max = Math.Max(resultData.Max, data.Max);
        }
    }

    static unsafe void WorkerLoop(
        byte* filePtr,
        long fileSize,
        ref long cursor,
        ref long totalEntries,
        int chunkSize,
        Dictionary<Utf8String, StationData> result,
        List<int> searchIterations)
    {
#if USE_BUILTIN_MAP
        Dictionary<Utf8String, StationData> table = new(TableSize);
#else
        StationEntry[] table = new StationEntry[TableSize];
#endif
        int maxSearchIterations = 0;
        long entries = 0;

        long chunkIdx = 0;
        while ((chunkIdx = Interlocked.Add(ref cursor, chunkSize) - chunkSize) < fileSize)
        {
            int len = (int)Math.Min(chunkSize, fileSize - chunkIdx);
            Console.WriteLine($"Parsing chunk at {chunkIdx:n0} of size {len:n0}");
            bool isLineStart = chunkIdx == 0 || filePtr[chunkIdx - 1] == (byte)'\n';
            // Make the span extend over the chunk end to read the cropped line
            ReadOnlySpan<byte> span = new(
                filePtr + chunkIdx,
                (int)Math.Min(fileSize - chunkIdx, int.MaxValue));
            entries += ParseChunk(span, len, table, isLineStart, ref maxSearchIterations);
        }

        Interlocked.Add(ref totalEntries, entries);
        lock (result)
            AccumulateResult(result, table);
        lock (searchIterations)
            searchIterations.Add(maxSearchIterations);
    }

    static long ParseChunk(
        ReadOnlySpan<byte> span,
        int length,
#if USE_BUILTIN_MAP
        Dictionary<Utf8String, StationData> table,
#else
        StationEntry[] table,
#endif
        bool isLineStart,
        ref int maxSearchIterations)
    {
        long totalEntries = 0;
        int idx = 0;
        if (!isLineStart)
            idx = span.IndexOf((byte)'\n') + 1;

        while (idx < length)
        {
            int newLineIdx = span.Slice(idx).IndexOf((byte)'\n');
            if (newLineIdx == -1)
                break;
            newLineIdx += idx;

            ReadOnlySpan<byte> line = span.Slice(idx, newLineIdx - idx);

            int start = 0;
            int sepIdx = line.IndexOf((byte)';');

            ReadOnlySpan<byte> name = line.Slice(0, sepIdx);
            start = sepIdx + 1;

            int temperature = ParseToInt(line.Slice(start));

#if USE_BUILTIN_MAP
            ref StationData data = ref FindEntry(table, name);
#else
            ref StationData data = ref FindEntry(table, name, ref maxSearchIterations).Data;
#endif
            data.Sum += temperature;
            data.Count++;
            data.Min = Math.Min(data.Min, temperature);
            data.Max = Math.Max(data.Max, temperature);


            idx = newLineIdx + 1;
            totalEntries++;
        }

        return totalEntries;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int DoHash(ReadOnlySpan<byte> name)
    {
#if USE_BUILTIN_HASH
        HashCode hasher = new();
        hasher.AddBytes(name);
        return hasher.ToHashCode();
#else
        int hash = 0;
        int i = 0;
        while (i + 4 <= name.Length)
        {
            hash += Unsafe.As<byte, int>(ref MemoryMarshal.GetReference(name));
            i += 4;
        }
        while (i < name.Length)
            hash += name[i++];
        return hash;
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ref StationData FindEntry(
        Dictionary<Utf8String, StationData> table,
        ReadOnlySpan<byte> nameSpan)
    {
        int hash = DoHash(nameSpan);
        Utf8String name = new(hash, nameSpan);
        ref StationData entry = ref CollectionsMarshal.GetValueRefOrAddDefault(table, name, out bool exists);
        if (!exists)
            entry = new();
        return ref entry;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ref StationEntry FindEntry(
        StationEntry[] table,
        ReadOnlySpan<byte> nameSpan,
        ref int maxSearchIterations)
    {
        int hash = DoHash(nameSpan);
        Utf8String name = new(hash, nameSpan);
        int i = hash & (TableSize - 1);
        ref StationEntry entry = ref table[i];
        int searchIterations = 0;
        while (true)
        {
            entry = ref table[i];
            searchIterations++;
            if (searchIterations > TableSize)
                throw new Exception("Table overflowed");
            if (entry.Name.Length == 0)
                break;
            if (entry.Name == name)
                break;
            i = (i + 1) & (TableSize - 1);
        }
        if (entry.Name.Length == 0)
            entry = new(name);
        maxSearchIterations = Math.Max(maxSearchIterations, searchIterations);
        return ref entry;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int ParseToInt(ReadOnlySpan<byte> span)
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

    static void Verify(Dictionary<string, StationData> result, string resultPath, long correctMeasurementCount)
    {
        Dictionary<string, StationData> official = File.ReadAllLines(resultPath)
            .Select(line =>
            {
                string[] name = line.Split('=', 2);
                string[] data = name[^1].Split('/', 3);
                StationData sd = new();
                sd.Min = (long)(double.Parse(data[0]) * 10);
                sd.Avg = double.Parse(data[1]);
                sd.Max = (long)(double.Parse(data[2]) * 10);
                return (name[0], sd);
            })
            .ToDictionary();
        if (official.Count != result.Count)
            Console.WriteLine($"! station count should be {official.Count} but is {result.Count}");
        long measurementCount = result.Sum(e => e.Value.Count);
        if (measurementCount != correctMeasurementCount)
            Console.WriteLine($"! measurement count should be {correctMeasurementCount} but is {measurementCount}");
        List<string> missingStations = official.Keys.Except(result.Keys).Order().ToList();
        foreach (string s in missingStations)
            Console.WriteLine($"! missing station ({Encoding.UTF8.GetByteCount(s)}) \"{s}\"");
        List<string> extraStations = result.Keys.Except(official.Keys).Order().ToList();
        foreach (string s in extraStations)
            Console.WriteLine($"! extra station ??? \"{s}\"");
        var sortedOfficial = official.Keys.Except(missingStations).Order().ToList();
        foreach (string name in sortedOfficial)
        {
            StationData a = official[name];
            StationData b = result[name];
            if (!(a.Min == b.Min && a.Avg == b.Avg && a.Max == b.Max))
            {
                string sa = $"{a.Min / 10.0:n1}/{a.Avg:n1}/{a.Max / 10.0:n1}";
                string sb = $"{b.Min / 10.0:n1}/{b.Avg:n1}/{b.Max / 10.0:n1}";
                Console.WriteLine($"! \"{name}\" should be {sa} but is {sb}");
            }
        }
    }
}
