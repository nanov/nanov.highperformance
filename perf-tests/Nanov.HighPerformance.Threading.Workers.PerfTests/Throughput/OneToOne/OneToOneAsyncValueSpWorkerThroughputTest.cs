namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.OneToOne;

using System.Runtime.CompilerServices;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Zero-copy unicast through <see cref="AsyncValueSpWorker{T, THandler}" /> with a sync-completing
///   handler — the async + zero-copy combination.
/// </summary>
public sealed class OneToOneAsyncValueSpWorkerThroughputTest : IThroughputTest {
	private const int BufferSize = 1024 * 64;
	private const long Iterations = 1000L * 1000L * 100L;
	private static readonly long ExpectedResult = PerfTestUtil.AccumulatedAddition(Iterations);

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneToOneAsyncValueSpWorkerThroughputTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	[MethodImpl(MethodImplOptions.AggressiveOptimization)]
	public long Run(ThroughputSessionContext sessionContext) {
		var accumulator = new Accumulator();
		using var worker = new AsyncValueSpWorker<PerfValueEvent, AdditionAsyncValueWorkerHandler>(
			new AdditionAsyncValueWorkerHandler(accumulator, _consumerCpu), BufferSize);
		worker.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			sessionContext.Start();

			for (var i = 0L; i < Iterations; i++) {
				using var scope = worker.Publish();
				scope.Event().Value = i;
			}

			worker.TryComplete();
			worker.Completion.GetAwaiter().GetResult();

			sessionContext.Stop();
		}

		PerfTestUtil.FailIfNot(ExpectedResult, accumulator.Sum,
			$"Handler should have summed to {ExpectedResult}, but was {accumulator.Sum}");
		return Iterations;
	}
}
