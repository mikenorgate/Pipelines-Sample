using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Tasks;

namespace PipelinesSample
{
    public class DelimiterSeparatedValueParser : IAsyncDisposable
    {
        private const byte NewLineDelimiter = (byte) '\n';

        private readonly Encoding _encoding;
        private bool _atEndOfLine;
        private bool _columnActive;
        private string[] _columnNames;
        private int _currentColumn;
        private bool _endOfPipe;
        private readonly Task _fillPipeTask;
        private bool _initialised;
        private readonly Pipe _pipe;
        private readonly ValueBuffer _valueBuffer;
        private readonly byte _delimiter;

        public DelimiterSeparatedValueParser(Stream input, char delimiter, Encoding encoding = default)
        {
            _encoding = encoding ?? Encoding.UTF8;
            _delimiter = (byte) delimiter;
            _valueBuffer = new ValueBuffer();
            _pipe = new Pipe();
            _fillPipeTask = Task.Factory.StartNew(() => FillPipeAsync(_pipe.Writer, input));
        }

        public string CurrentColumn
        {
            get
            {
                if (!_columnActive)
                    return null;

                return _columnNames[_currentColumn];
            }
        }

        public string CurrentValue
        {
            get
            {
                if (!_columnActive)
                    return null;

                return _encoding.GetString(_valueBuffer.GetReadOnlySequence());
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _pipe.Reader.CompleteAsync().ConfigureAwait(false);
            await _fillPipeTask.ConfigureAwait(false);
        }

        public async ValueTask<bool> MoveNextRecord()
        {
            if (_endOfPipe)
                return false;

            if (!_initialised)
                await Initialise().ConfigureAwait(false);

            _columnActive = false;
            _currentColumn = -1;

            if (!_atEndOfLine)
            {
                bool complete;
                do
                {
                    var readResult = await _pipe.Reader.ReadAsync().ConfigureAwait(false);

                    complete = AdvancePastEndOfLine(readResult.Buffer, out var position);

                    if (readResult.IsCompleted)
                    {
                        _endOfPipe = true;
                        return false;
                    }

                    _pipe.Reader.AdvanceTo(position);
                } while (!complete);
            }
            else
            {
                _atEndOfLine = false;
            }

            return !_endOfPipe;
        }

        private async ValueTask Initialise()
        {
            _initialised = true;

            var names = new List<string>();
            while (await MoveNextColumn()) 
                names.Add(CurrentValue);

            _columnNames = names.ToArray();
        }

        public async ValueTask<bool> MoveNextColumn()
        {
            if (!_initialised)
                return false;

            if (_endOfPipe)
                return false;

            if (_atEndOfLine)
                return false;

            _valueBuffer.Reset();

            bool complete;
            do
            {
                var readResult = await _pipe.Reader.ReadAsync().ConfigureAwait(false);

                complete = ReadColumn(readResult.Buffer, out var position);

                if (readResult.IsCompleted)
                {
                    _endOfPipe = true;
                    return false;
                }

                _pipe.Reader.AdvanceTo(position);
            } while (!complete);

            _columnActive = !_endOfPipe;
            _currentColumn++;

            return !_endOfPipe;
        }

        private bool AdvancePastEndOfLine(in ReadOnlySequence<byte> sequence, out SequencePosition position)
        {
            var reader = new SequenceReader<byte>(sequence);

            while (!reader.End)
            {
                var remaining = reader.UnreadSpan;
                var index = remaining.IndexOf(NewLineDelimiter);

                if (index >= 0)
                {
                    reader.Advance(index + 1);
                    position = reader.Position;
                    return true;
                }

                _valueBuffer.Append(remaining);
                reader.Advance(remaining.Length);
            }

            position = reader.Position;
            return false;
        }

        private bool ReadColumn(in ReadOnlySequence<byte> sequence, out SequencePosition position)
        {
            var reader = new SequenceReader<byte>(sequence);

            while (!reader.End)
            {
                var remaining = reader.UnreadSpan;
                var index = remaining.IndexOfAny(NewLineDelimiter, _delimiter);

                if (index >= 0)
                {
                    var span = remaining.Slice(0, index);
                    _valueBuffer.Append(span);
                    if (remaining[index] == NewLineDelimiter)
                        _atEndOfLine = true;
                    reader.Advance(index + 1);
                    position = reader.Position;
                    return true;
                }

                _valueBuffer.Append(remaining);
                reader.Advance(remaining.Length);
            }

            position = reader.Position;
            return false;
        }


        private static async Task FillPipeAsync(PipeWriter writer, Stream input)
        {
            while (true)
            {
                int read;
                var memory = writer.GetMemory();
                if ((read = await input.ReadAsync(memory)) == 0)
                    break;

                writer.Advance(read);

                var result = await writer.FlushAsync().ConfigureAwait(false);

                if (result.IsCompleted) break;
            }
            
            await writer.CompleteAsync().ConfigureAwait(false);
        }


        internal class ValueBuffer : IDisposable
        {
            private MemorySegment _first;
            private MemorySegment _last;

            public void Dispose()
            {
                Reset();
            }

            public void Append(ReadOnlySpan<byte> span)
            {
                if (_first == null)
                    _last = _first = new MemorySegment(span);
                else
                    _last = _first.Append(span);
            }

            public void Reset()
            {
                _first?.Dispose();
                _first = null;
            }

            public ReadOnlySequence<byte> GetReadOnlySequence()
            {
                return new(_first, 0, _last, _last.Memory.Length);
            }


            internal class MemorySegment : ReadOnlySequenceSegment<byte>, IDisposable
            {
                private readonly byte[] _array;

                public MemorySegment(ReadOnlySpan<byte> memory)
                {
                    _array = ArrayPool<byte>.Shared.Rent(memory.Length);
                    Memory = _array.AsMemory(0, memory.Length);
                    memory.CopyTo(_array);
                }

                public void Dispose()
                {
                    ((MemorySegment) Next)?.Dispose();
                    ArrayPool<byte>.Shared.Return(_array);
                }

                public MemorySegment Append(ReadOnlySpan<byte> memory)
                {
                    var segment = new MemorySegment(memory)
                    {
                        RunningIndex = RunningIndex + Memory.Length
                    };

                    Next = segment;

                    return segment;
                }
            }
        }
    }
}