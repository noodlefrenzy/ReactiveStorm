using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;
using System.Diagnostics;

namespace ReactiveStorm
{
	[Active(true)]
	class Program : TopologyDescriptor
	{
		static void Main(string[] args)
		{
			Trace.Listeners.Add(new ConsoleTraceListener());
			Console.WriteLine("Starting tests");
			//System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "WordCount-LocalTest");
			// Initialize the runtime
			SCPRuntime.Initialize();

			//If we are not running under the local context, throw an error
			if (Context.pluginType != SCPPluginType.SCP_NET_LOCAL)
			{
				throw new Exception(string.Format("unexpected pluginType: {0}", Context.pluginType));
			}
			RunLocalTest();

			Console.WriteLine("Tests finished");
			Console.ReadKey();
		}

		static void RunLocalTest()
		{
			#region Test the spout
			Console.WriteLine("Starting RxIntSpout");
			LocalContext spoutCtx = LocalContext.Get();
			RxIntSpout seqSpout = new RxIntSpout(spoutCtx, null);

			while (!seqSpout.FinishedTransmitting)
			{
				seqSpout.NextTuple(null);
				//Task.Delay(100).Wait();
			}
			spoutCtx.WriteMsgQueueToFile("seq.txt");
			Console.WriteLine("RxIntSpout finished");
			#endregion

			#region Test the rx bolt
			Console.WriteLine("Starting RxMovingAverageBolt");
			LocalContext rxCtx = LocalContext.Get();
			var rxBolt = new RxMovingAverageBolt(rxCtx);

			rxCtx.ReadFromFileToMsgQueue("seq.txt");
			rxCtx.RecvFromMsgQueue().ToList()
				.ForEach(tuple => rxBolt.Execute(tuple));
			rxCtx.WriteMsgQueueToFile("rx.txt");
			Console.WriteLine("RxMovingAverageBolt finished");
			#endregion
		}

		public ITopologyBuilder GetTopologyBuilder()
		{
			TopologyBuilder topologyBuilder = new TopologyBuilder("ReactiveStorm");
			topologyBuilder.SetSpout(
				"Spout",
				RxIntSpout.Get,
				new Dictionary<string, List<string>>()
				{
					{Constants.DEFAULT_STREAM_ID, new List<string>(){"count"}}
				},
				1);
			topologyBuilder.SetBolt(
				"Bolt",
				RxMovingAverageBolt.Get,
				new Dictionary<string, List<string>>()
				{
					{Constants.DEFAULT_STREAM_ID, new List<string>() { "rollingAverage" } }
				},
                1).shuffleGrouping("Spout");

			return topologyBuilder;
		}
	}
}

