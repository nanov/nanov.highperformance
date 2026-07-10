namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.OneToOne;

using System.Runtime.CompilerServices;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Keyed throughput through <see cref="ConflationSpWorker{TKey, TValue, THandler}" /> (last-wins). The
///   producer dispatches over a fixed key space, publishing a fresh reference carrier per message (the
///   worker's <c>TValue</c> must be a class, and its null-clear distinguishes overrides by reference
///   identity — so reusing one carrier per key would be incorrect). A lagging consumer sees only the
///   latest value per key; since conflation publishes immutable snapshots, the per-message allocation is
///   the realistic cost (it shows up in the GC column). Correctness: every key's final value is
///   delivered during drain. No Disruptor analog.
/// </summary>
public sealed class OneToOneConflationSpWorkerThroughputTest : IThroughputTest {
	private const int KeyCount = 1024;
	private const int MapCapacity = 4096;
	private const long Iterations = 1000L * 1000L * 100L;

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneToOneConflationSpWorkerThroughputTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	[MethodImpl(MethodImplOptions.AggressiveOptimization)]
	public long Run(ThroughputSessionContext sessionContext) {
		var accumulator = new Accumulator(KeyCount);

		using var worker = new ConflationSpWorker<long, LongRef, LastWinsConflationHandler>(
			new LastWinsConflationHandler(accumulator, _consumerCpu), MapCapacity);
		worker.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			sessionContext.Start();

			for (var i = 0L; i < Iterations; i++)
				worker.Dispatch((int)(i % KeyCount), new LongRef { Value = i });

			worker.TryComplete();
			worker.Completion.GetAwaiter().GetResult();

			sessionContext.Stop();
		}

		// Each key's last dispatched value is Iterations - KeyCount + key (Iterations is a multiple of
		// KeyCount). Verify every key's final value was delivered.
		var expected = 0L;
		var actual = 0L;
		for (var k = 0; k < KeyCount; k++) {
			expected += Iterations - KeyCount + k;
			actual += accumulator.LastByKey[k];
		}

		PerfTestUtil.FailIfNot(expected, actual,
			$"Final per-key values should sum to {expected}, but was {actual}");
		return Iterations;
	}
}
