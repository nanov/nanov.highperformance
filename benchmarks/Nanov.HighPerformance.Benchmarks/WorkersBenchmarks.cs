namespace Nanov.HighPerformance.Benchmarks;

using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Nanov.HighPerformance.Threading.Workers;

// ================================================================================================
//  Nanov.HighPerformance.Threading.Workers — SpWorker family vs System.Threading.Channels.
//  Matrix: {sync, async} handler × {value (struct), reference (class)} item, at ring capacity 24
//  and 1024, pushing 1,000,000 items through a single consumer. Zero-copy ValueSpWorker rows appear
//  for the struct cases. The channel row is the baseline in each group.
// ================================================================================================

// 32-byte value payload — big enough that copy (SpWorker) vs zero-copy (ValueSpWorker) shows.
public struct ValueMsg {
	public long Key;
	public long Value;
	public long Timestamp;
	public long Flags;
}

// Reference payload with the same shape — dispatched as a reused singleton (measures the queue, not
// message allocation).
public sealed class RefMsg {
	public long Key;
	public long Value;
	public long Timestamp;
	public long Flags;
}

// Mutable reference state so the by-value struct handlers' accumulation is observable (returned from
// each benchmark to defeat dead-code elimination).
public sealed class Box {
	public long Sum;
	public int Calls; // per-item counter for the mixed handler (single consumer → no lock)
}

// --- copy-based handlers ------------------------------------------------------------------------

public readonly struct ValueSumHandler : IWorkerHandler<ValueMsg> {
	private readonly Box _box;
	public ValueSumHandler(Box box) => _box = box;
	public void Handle(ValueMsg item) => _box.Sum += item.Key;
	public void OnError(Exception error) { }
	public void OnCompleted(Exception? error) { }
}

public readonly struct RefSumHandler : IWorkerHandler<RefMsg> {
	private readonly Box _box;
	public RefSumHandler(Box box) => _box = box;
	public void Handle(RefMsg item) => _box.Sum += item.Key;
	public void OnError(Exception error) { }
	public void OnCompleted(Exception? error) { }
}

// async twins — HandleAsync completes synchronously (returns default), so these measure the
// async-worker machinery overhead, not await latency. OnError stays synchronous.

public readonly struct AsyncValueSumHandler : IAsyncWorkerHandler<ValueMsg> {
	private readonly Box _box;
	public AsyncValueSumHandler(Box box) => _box = box;
	public ValueTask HandleAsync(ValueMsg item, CancellationToken cancellationToken) {
		_box.Sum += item.Key;
		return HandleAsyncAwaiter();
	}
	private async ValueTask HandleAsyncAwaiter() => await Task.Yield();
	public void OnError(Exception error) { }
	public ValueTask OnCompletedAsync(Exception? error) => default;
}

public readonly struct AsyncRefSumHandler : IAsyncWorkerHandler<RefMsg> {
	private readonly Box _box;
	public AsyncRefSumHandler(Box box) => _box = box;
	public ValueTask HandleAsync(RefMsg item, CancellationToken cancellationToken) {
		_box.Sum += item.Key;
		return HandleAsyncAwaiter();
	}
	private async ValueTask HandleAsyncAwaiter() => await Task.Yield();

	public void OnError(Exception error) { }
	public ValueTask OnCompletedAsync(Exception? error) => default;
}

// MIXED workload: 2 of every 3 items complete synchronously (return default); every 3rd truly
// suspends (Task.Yield). Models a "mostly cached, occasionally async IO" handler.
public readonly struct MixedAsyncRefHandler : IAsyncWorkerHandler<RefMsg> {
	private readonly Box _box;
	public MixedAsyncRefHandler(Box box) => _box = box;
	public ValueTask HandleAsync(RefMsg item, CancellationToken cancellationToken) {
		_box.Sum += item.Key;
		return ++_box.Calls % 3 == 0 ? HandleAsyncAwaiter() : default;
	}
	private static async ValueTask HandleAsyncAwaiter() => await Task.Yield();

	public void OnError(Exception error) { }
	public ValueTask OnCompletedAsync(Exception? error) => default;
}

// --- zero-copy scope handlers (value items only) ------------------------------------------------

public readonly struct ScopeSumHandler : IValueWorkerHandler<ValueMsg> {
	private readonly Box _box;
	public ScopeSumHandler(Box box) => _box = box;
	public void Handle(ref HandleScope<ValueMsg> scope) {
		_box.Sum += scope.Event().Key;
		scope.Release();
	}
	public void OnError(Exception error) { }
	public void OnCompleted(Exception? error) { }
}

public readonly struct AsyncScopeSumHandler : IAsyncValueWorkerHandler<ValueMsg> {
	private readonly Box _box;
	public AsyncScopeSumHandler(Box box) => _box = box;
	public ValueTask HandleAsync(ref HandleScope<ValueMsg> scope, CancellationToken cancellationToken) {
		_box.Sum += scope.Event().Key;
		scope.Release();
		return HandleAsyncAwaiter();
	}
	private async ValueTask HandleAsyncAwaiter() => await Task.Yield();

	public void OnError(Exception error) { }
	public ValueTask OnCompletedAsync(Exception? error) => default;
}

// ================================================================================================
//  Benchmark groups (separate classes so each gets its own Channel baseline / ratio).
// ================================================================================================

/// <summary>Value (struct) item, SYNC handler: Channel vs copy <see cref="SpWorker{T, THandler}" /> vs zero-copy <see cref="ValueSpWorker{T, THandler}" />.</summary>
[MemoryDiagnoser]
public class SyncValueBenchmarks {
	private static readonly ValueMsg Msg = new() { Key = 1, Value = 2, Timestamp = 3, Flags = 4 };

	[Params(24, 1024)]
	public int Capacity;

	[Params(1_000_000)]
	public int ItemCount;

	[Benchmark(Baseline = true)]
	public async Task<long> Channel_Value() {
		var channel = Channel.CreateBounded<ValueMsg>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});

		long sum = 0;
		var consumer = Task.Run(async () => {
			var reader = channel.Reader;
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
				while (reader.TryRead(out var item))
					sum += item.Key;
		});

		var writer = channel.Writer;
		var msg = Msg;
		for (var i = 0; i < ItemCount; i++)
			await writer.WriteAsync(msg).ConfigureAwait(false);
		writer.Complete();

		await consumer.ConfigureAwait(false);
		return sum;
	}

	[Benchmark]
	public long SpWorker_Value() {
		var box = new Box();
		using var w = new SpWorker<ValueMsg, ValueSumHandler>(new ValueSumHandler(box), Capacity);
		w.Start();

		var msg = Msg;
		for (var i = 0; i < ItemCount; i++)
			w.Dispatch(msg);

		w.TryComplete();
		w.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}

	[Benchmark]
	public long ValueSpWorker_Value() {
		var box = new Box();
		using var w = new ValueSpWorker<ValueMsg, ScopeSumHandler>(new ScopeSumHandler(box), Capacity);
		w.Start();

		var msg = Msg;
		for (var i = 0; i < ItemCount; i++) {
			using var s = w.Publish();
			s.Event() = msg; // write in place — no struct copy
		}

		w.TryComplete();
		w.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}
}

/// <summary>Value (struct) item, ASYNC handler (sync-completing): Channel vs copy <see cref="AsyncSpWorker{T, THandler}" /> vs zero-copy <see cref="AsyncValueSpWorker{T, THandler}" />.</summary>
[MemoryDiagnoser]
public class AsyncValueBenchmarks {
	private static readonly ValueMsg Msg = new() { Key = 1, Value = 2, Timestamp = 3, Flags = 4 };

	[Params(24)]
	public int Capacity;

	[Params(1_000)]
	public int ItemCount;

	[Benchmark(Baseline = true)]
	public async Task<long> Channel_Value() {
		var channel = Channel.CreateBounded<ValueMsg>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});

		var box = new Box();
		var handler = new AsyncValueSumHandler(box);
		var consumer = Task.Run(async () => {
			var reader = channel.Reader;
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
			while (reader.TryRead(out var item)) {
				await handler.HandleAsync(item, CancellationToken.None);
			}
		});

		var writer = channel.Writer;
		var msg = Msg;
		for (var i = 0; i < ItemCount; i++)
			await writer.WriteAsync(msg).ConfigureAwait(false);
		writer.Complete();

		await consumer.ConfigureAwait(false);
		return box.Sum;
	}

	[Benchmark]
	public long AsyncSpWorker_Value() {
		var box = new Box();
		using var w = new AsyncSpWorker<ValueMsg, AsyncValueSumHandler>(new AsyncValueSumHandler(box), Capacity);
		w.Start();

		var msg = Msg;
		for (var i = 0; i < ItemCount; i++)
			w.Dispatch(msg);

		w.TryComplete();
		w.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}

	[Benchmark]
	public long AsyncValueSpWorker_Value() {
		var box = new Box();
		using var w = new AsyncValueSpWorker<ValueMsg, AsyncScopeSumHandler>(new AsyncScopeSumHandler(box), Capacity);
		w.Start();

		var msg = Msg;
		for (var i = 0; i < ItemCount; i++) {
			using var s = w.Publish();
			s.Event() = msg;
		}

		w.TryComplete();
		w.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}
}

/// <summary>Reference (class) item, SYNC handler: Channel vs copy <see cref="SpWorker{T, THandler}" /> (no zero-copy variant — needs <c>T : struct</c>).</summary>
[MemoryDiagnoser]
public class SyncRefBenchmarks {
	private static readonly RefMsg Ref = new() { Key = 1, Value = 2, Timestamp = 3, Flags = 4 };

	[Params(24, 1024)]
	public int Capacity;

	[Params(1_000_000)]
	public int ItemCount;

	[Benchmark(Baseline = true)]
	public async Task<long> Channel_Ref() {
		var channel = Channel.CreateBounded<RefMsg>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});

		long sum = 0;
		var consumer = Task.Run(async () => {
			var reader = channel.Reader;
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
				while (reader.TryRead(out var item))
					sum += item.Key;
		});

		var writer = channel.Writer;
		var msg = Ref;
		for (var i = 0; i < ItemCount; i++)
			await writer.WriteAsync(msg).ConfigureAwait(false);
		writer.Complete();

		await consumer.ConfigureAwait(false);
		return sum;
	}

	[Benchmark]
	public long SpWorker_Ref() {
		var box = new Box();
		using var w = new SpWorker<RefMsg, RefSumHandler>(new RefSumHandler(box), Capacity);
		w.Start();

		var msg = Ref;
		for (var i = 0; i < ItemCount; i++)
			w.Dispatch(msg);

		w.TryComplete();
		w.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}
}

/// <summary>Reference (class) item, ASYNC handler (sync-completing): Channel vs copy <see cref="AsyncSpWorker{T, THandler}" />.</summary>
[MemoryDiagnoser]
public class AsyncRefBenchmarks {
	private static readonly RefMsg Ref = new() { Key = 1, Value = 2, Timestamp = 3, Flags = 4 };

	[Params(24)]
	public int Capacity;

	[Params(1_000)]
	public int ItemCount;

	[Benchmark(Baseline = true)]
	public async Task<long> Channel_Ref() {
		var channel = Channel.CreateBounded<RefMsg>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});

		var box = new Box();
		var handler = new AsyncRefSumHandler(box);
		var consumer = Task.Run(async () => {
			var reader = channel.Reader;
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
			while (reader.TryRead(out var item))
				await handler.HandleAsync(item, CancellationToken.None);
		});

		var writer = channel.Writer;
		var msg = Ref;
		for (var i = 0; i < ItemCount; i++)
			await writer.WriteAsync(msg).ConfigureAwait(false);
		writer.Complete();

		await consumer.ConfigureAwait(false);
		return box.Sum;
	}

	[Benchmark]
	public long AsyncSpWorker_Ref() {
		var box = new Box();
		using var w = new AsyncSpWorker<RefMsg, AsyncRefSumHandler>(new AsyncRefSumHandler(box), Capacity);
		w.Start();

		var msg = Ref;
		for (var i = 0; i < ItemCount; i++)
			w.Dispatch(msg);

		w.TryComplete();
		w.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}
}

/// <summary>
///   Reference (class) item, MIXED async handler: 2 of every 3 items complete synchronously, every
///   3rd truly suspends. <c>Channel&lt;T&gt;</c> vs <see cref="AsyncSpWorker{T, THandler}" /> — shows
///   how the worker handles a "mostly sync, occasionally async" workload.
/// </summary>
[MemoryDiagnoser]
public class MixedRefBenchmarks {
	private static readonly RefMsg Ref = new() { Key = 1, Value = 2, Timestamp = 3, Flags = 4 };

	[Params(24)]
	public int Capacity;

	[Params(10_000)]
	public int ItemCount;

	[Benchmark(Baseline = true)]
	public async Task<long> Channel_Ref() {
		var channel = Channel.CreateBounded<RefMsg>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});

		var box = new Box();
		var handler = new MixedAsyncRefHandler(box);
		var consumer = Task.Run(async () => {
			var reader = channel.Reader;
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
			while (reader.TryRead(out var item))
				await handler.HandleAsync(item, CancellationToken.None);
		});

		var writer = channel.Writer;
		var msg = Ref;
		for (var i = 0; i < ItemCount; i++)
			await writer.WriteAsync(msg).ConfigureAwait(false);
		writer.Complete();

		await consumer.ConfigureAwait(false);
		return box.Sum;
	}

	[Benchmark]
	public long AsyncSpWorker_Ref() {
		var box = new Box();
		using var w = new AsyncSpWorker<RefMsg, MixedAsyncRefHandler>(new MixedAsyncRefHandler(box), Capacity);
		w.Start();

		var msg = Ref;
		for (var i = 0; i < ItemCount; i++)
			w.Dispatch(msg);

		w.TryComplete();
		w.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}
}
