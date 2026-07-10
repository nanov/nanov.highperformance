namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

using System;
using System.Threading;
using System.Threading.Tasks;

// Struct handlers that accumulate the dispatched values into a shared Accumulator. They are stored
// by value in the worker and dispatched through a constrained generic call, so the JIT devirtualizes
// and inlines Handle on the hot path. The first Handle optionally self-pins the worker thread to a
// CPU (the consumer-side affinity Disruptor applies via its EventProcessor); after that the check is
// a single predicted branch.

/// <summary>Copy-based sync handler: sums <see cref="long" /> items.</summary>
public struct AdditionWorkerHandler : IWorkerHandler<long> {
	private readonly Accumulator _acc;
	private readonly int? _consumerCpu;

	public AdditionWorkerHandler(Accumulator acc, int? consumerCpu) {
		_acc = acc;
		_consumerCpu = consumerCpu;
	}

	public void Handle(long item) {
		if (!_acc.Pinned) {
			// Match Disruptor: the consumer thread runs at Highest priority (steers it to a performance core
			// on heterogeneous CPUs); pin to a CPU too when a custom set was given.
			ThreadAffinityUtil.PrimeConsumerThread(_consumerCpu);
			_acc.Pinned = true;
		}

		_acc.Sum += item;
	}

	public void OnError(Exception error) => System.Console.Error.WriteLine(error);
	public void OnCompleted(Exception? error) { }
}

/// <summary>Zero-copy sync handler: sums the slot's value, releasing the slot first.</summary>
public struct AdditionValueWorkerHandler : IValueWorkerHandler<PerfValueEvent> {
	private readonly Accumulator _acc;
	private readonly int? _consumerCpu;

	public AdditionValueWorkerHandler(Accumulator acc, int? consumerCpu) {
		_acc = acc;
		_consumerCpu = consumerCpu;
	}

	public void Handle(ref HandleScope<PerfValueEvent> scope) {
		if (_consumerCpu is { } cpu && !_acc.Pinned) {
			ThreadAffinityUtil.PinCurrentThread(cpu);
			_acc.Pinned = true;
		}

		_acc.Sum += scope.Event().Value;
		scope.Release();
	}

	public void Clear(ref PerfValueEvent value) { } // blittable slot — nothing to release
	public void OnError(Exception error) => System.Console.Error.WriteLine(error);
	public void OnCompleted(Exception? error) { }
}

/// <summary>Copy-based async handler, sync-completing (measures the async machinery, not await latency).</summary>
public struct AdditionAsyncWorkerHandler : IAsyncWorkerHandler<long> {
	private readonly Accumulator _acc;
	private readonly int? _consumerCpu;

	public AdditionAsyncWorkerHandler(Accumulator acc, int? consumerCpu) {
		_acc = acc;
		_consumerCpu = consumerCpu;
	}

	public ValueTask HandleAsync(long item, CancellationToken cancellationToken) {
		if (_consumerCpu is { } cpu && !_acc.Pinned) {
			ThreadAffinityUtil.PinCurrentThread(cpu);
			_acc.Pinned = true;
		}

		_acc.Sum += item;
		return default;
	}

	public void OnError(Exception error) => System.Console.Error.WriteLine(error);
	public ValueTask OnCompletedAsync(Exception? error) => default;
}

/// <summary>Zero-copy async handler, sync-completing.</summary>
public struct AdditionAsyncValueWorkerHandler : IAsyncValueWorkerHandler<PerfValueEvent> {
	private readonly Accumulator _acc;
	private readonly int? _consumerCpu;

	public AdditionAsyncValueWorkerHandler(Accumulator acc, int? consumerCpu) {
		_acc = acc;
		_consumerCpu = consumerCpu;
	}

	public ValueTask HandleAsync(ref HandleScope<PerfValueEvent> scope, CancellationToken cancellationToken) {
		if (_consumerCpu is { } cpu && !_acc.Pinned) {
			ThreadAffinityUtil.PinCurrentThread(cpu);
			_acc.Pinned = true;
		}

		_acc.Sum += scope.Event().Value;
		scope.Release();
		return default;
	}

	public void Clear(ref PerfValueEvent value) { }
	public void OnError(Exception error) => System.Console.Error.WriteLine(error);
	public ValueTask OnCompletedAsync(Exception? error) => default;
}

/// <summary>Coalescing (exactly-once additive) handler: folds values per key, sums on delivery.</summary>
public struct SummingCoalescingHandler : ICoalescingWorkerHandler<long, long> {
	private readonly Accumulator _acc;
	private readonly int? _consumerCpu;

	public SummingCoalescingHandler(Accumulator acc, int? consumerCpu) {
		_acc = acc;
		_consumerCpu = consumerCpu;
	}

	// Runs on the PRODUCER thread under the cell lock — additive merge, default(long)=0 is the identity.
	public long Coalesce(ref long current, ref long incoming) => current + incoming;

	public void Handle(long key, long value) {
		if (_consumerCpu is { } cpu && !_acc.Pinned) {
			ThreadAffinityUtil.PinCurrentThread(cpu);
			_acc.Pinned = true;
		}

		_acc.Sum += value; // exactly-once ⇒ the grand total equals the sum of all dispatched values
	}

	public void OnError(Exception error) => System.Console.Error.WriteLine(error);
	public void OnCompleted(Exception? error) { }
}

/// <summary>Conflation (last-wins) handler: records the latest value seen per key for verification.</summary>
public struct LastWinsConflationHandler : IConflationWorkerHandler<long, LongRef> {
	private readonly Accumulator _acc;
	private readonly int? _consumerCpu;

	public LastWinsConflationHandler(Accumulator acc, int? consumerCpu) {
		_acc = acc;
		_consumerCpu = consumerCpu;
	}

	public void Handle(long key, LongRef value) {
		if (_consumerCpu is { } cpu && !_acc.Pinned) {
			ThreadAffinityUtil.PinCurrentThread(cpu);
			_acc.Pinned = true;
		}

		_acc.Count++;
		// Last write wins. Each dispatch carries a fresh reference (the worker's null-clear distinguishes
		// overrides by identity), and the worker always delivers each key's final pending value during
		// drain, so LastByKey deterministically ends at each key's last dispatched value.
		_acc.LastByKey[(int)key] = value.Value;
	}

	public void OnError(Exception error) => System.Console.Error.WriteLine(error);
	public void OnCompleted(Exception? error) { }
}
