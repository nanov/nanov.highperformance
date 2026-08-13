namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Tests.Threading.Workers;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using NUnit.Framework;

/// <summary>
///   A fault on ANY background thread must resolve <c>Completion</c>, not hang the system.
///   <para>
///     Before this, a worker's catch recorded the exception but never set <c>_stopped</c>, and the
///     router's catch set a scheduler-private flag <c>ActorSystem</c> never read. Either way the
///     surviving workers found <c>IsQuiescent()</c> false — the dead thread's messages are never
///     processed — and re-parked on an unbounded wait. <c>_liveWorkers</c> never reached zero and
///     <c>Completion</c> never resolved. A hang is the worst possible failure mode: it reports
///     nothing and cannot be diagnosed from the outside.
///   </para>
/// </summary>
[TestFixture]
public class FaultPropagationTests {
	private static ActorSystem<int, int, RecordingHandler, RoutedScheduler> New(
		RecordingState state, int workers) =>
		new(new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 0, workers: workers, scheduler: new RoutedScheduler { Capacity = 256 });

	[Test]
	[CancelAfter(30_000)]
	public void A_Handler_Exception_Is_Not_A_Fault_And_Completion_Still_Succeeds() {
		// Guards the fault channel against being wired too aggressively. A throw from Handle is caught
		// per message, reported to OnError, and the message consumed — the worker continues. If that
		// ever reached Fault(), one bad message would tear the whole system down.
		var state = new RecordingState();
		state.Throw = (_, value) => value == 0;
		using var system = New(state, workers: 4);
		system.Start();

		for (var i = 0; i < 20; i++)
			Assert.That(system.Dispatch(i % 4, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 20);

		Assert.Multiple(() => {
			Assert.That(state.Errors, Is.Not.Empty, "the handler throw was never reported to OnError");
			Assert.That(system.Completion.IsCompletedSuccessfully, Is.True,
				"a handler exception must not fault Completion — it is not an escaping fault");
		});
	}

	[Test]
	[CancelAfter(30_000)]
	public void A_Background_Fault_Faults_Completion_With_That_Exception() {
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);

		// Injected deliberately. The router's only natural trigger is a misconfiguration that is now
		// bounds-checked, and a worker-loop escape has no natural trigger at all — so a hook is the
		// honest instrument. Contriving a "natural" trigger would make the test lie about its subject.
		system.InjectSchedulerFaultForTesting(new InvalidOperationException("router boom"));

		var agg = Assert.Throws<AggregateException>(() => system.Completion.Wait(TimeSpan.FromSeconds(10)));
		Assert.That(agg!.InnerException, Is.InstanceOf<InvalidOperationException>());
		Assert.That(agg.InnerException!.Message, Is.EqualTo("router boom"),
			"the fault was recorded but the wrong exception surfaced");
	}

	[Test]
	[CancelAfter(30_000)]
	public void A_Fault_Releases_Workers_Already_Parked_On_An_Unbounded_Wait() {
		// The case that actually hangs today: every worker is asleep on a wait with no timeout when the
		// fault arrives. Nothing else will ever signal them — no message will be processed, so no
		// publish, so no quiescence broadcast — which is why Fault() must set _stopped AND signal.
		var state = new RecordingState();
		using var system = New(state, workers: 8);
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(200); // all eight park

		system.InjectSchedulerFaultForTesting(new InvalidOperationException("boom while parked"));

		// Assert.Throws fails with "no exception" if Wait times out, so a hang is a red test, not a
		// hang — the bounded Wait is what makes that true.
		var agg = Assert.Throws<AggregateException>(() => system.Completion.Wait(TimeSpan.FromSeconds(10)));
		Assert.That(agg!.InnerException!.Message, Is.EqualTo("boom while parked"));
		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(8),
			"not every worker reached OnCompleted — some never woke");
	}
}
