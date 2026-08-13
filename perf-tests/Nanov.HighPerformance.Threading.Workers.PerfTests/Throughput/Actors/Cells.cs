namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>Scheduler B — push-side routing through a dedicated router thread.</summary>
public abstract class ActorsBTestBase : ActorThroughputTestBase {
	protected ActorsBTestBase(ProgramOptions options) : base(options) { }

	protected override long RunStream(int[] keys, ActorPerfState state, int? producerCpu) =>
		Drive(new RoutedScheduler { Capacity = Cardinality * 4 }, Cardinality, keys, state, producerCpu);
}

/// <summary>Scheduler D — pull-side work stealing.</summary>
public abstract class ActorsDTestBase : ActorThroughputTestBase {
	protected ActorsDTestBase(ProgramOptions options) : base(options) { }

	protected override long RunStream(int[] keys, ActorPerfState state, int? producerCpu) =>
		Drive(new WorkStealingScheduler { Capacity = Cardinality * 4, DequeCapacity = 256 },
			Cardinality, keys, state, producerCpu);
}

public sealed class ActorsBUniformCard64ThroughputTest : ActorsBTestBase {
	public ActorsBUniformCard64ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 64;
	protected override Skew Skew => Skew.Uniform;
}

public sealed class ActorsBUniformCard8192ThroughputTest : ActorsBTestBase {
	public ActorsBUniformCard8192ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 8192;
	protected override Skew Skew => Skew.Uniform;
}

public sealed class ActorsBZipfCard64ThroughputTest : ActorsBTestBase {
	public ActorsBZipfCard64ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 64;
	protected override Skew Skew => Skew.Zipf;
}

public sealed class ActorsBZipfCard8192ThroughputTest : ActorsBTestBase {
	public ActorsBZipfCard8192ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 8192;
	protected override Skew Skew => Skew.Zipf;
}

public sealed class ActorsDUniformCard64ThroughputTest : ActorsDTestBase {
	public ActorsDUniformCard64ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 64;
	protected override Skew Skew => Skew.Uniform;
}

public sealed class ActorsDUniformCard8192ThroughputTest : ActorsDTestBase {
	public ActorsDUniformCard8192ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 8192;
	protected override Skew Skew => Skew.Uniform;
}

public sealed class ActorsDZipfCard64ThroughputTest : ActorsDTestBase {
	public ActorsDZipfCard64ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 64;
	protected override Skew Skew => Skew.Zipf;
}

public sealed class ActorsDZipfCard8192ThroughputTest : ActorsDTestBase {
	public ActorsDZipfCard8192ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 8192;
	protected override Skew Skew => Skew.Zipf;
}
