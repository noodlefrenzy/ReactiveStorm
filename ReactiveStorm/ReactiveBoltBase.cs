using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using System.Reactive.Subjects;
using System.Diagnostics;

namespace ReactiveStorm
{
	/// <summary>
	/// Wrap the (converted) input to keep track of the original tuple for anchoring.
	/// </summary>
	public struct BoltInput<TIn>
	{
		public SCPTuple Original { get; set; }

		public TIn Converted { get; set; }
	}

	/// <summary>
	/// Wrap the result to keep track of all tuples that should anchor to the output.
	/// </summary>
	public struct BoltOutput<TOut>
	{
		public IEnumerable<SCPTuple> Anchors { get; set; }
		public TOut Result { get; set; }
	}

	public abstract class ReactiveBoltBase<TIn, TOut> : ISCPBolt
	{
		// Child classes should implement this getter for building the topology.
		//public static Bolt Get(Context ctx, Dictionary<string, Object> parms)
		//{
		//	return new Bolt(ctx);
		//}

		public ReactiveBoltBase(Context context)
		{
			this.Context = context;
			MapSchemas(this.Context);
			this.Input = new Subject<BoltInput<TIn>>();
			ProcessInput(this.Input).Subscribe(
				output =>
				{
					var converted = this.ConvertOutput(output.Result);
					var outputStreamId = this.OutputStreamId ?? Constants.DEFAULT_STREAM_ID;
					if (output.Anchors != null && output.Anchors.Any())
					{
						this.Context.Emit(outputStreamId, output.Anchors, converted);
					}
					else
					{
						this.Context.Emit(outputStreamId, converted);
					}
				},
				err =>
				{
					Trace.TraceError(err.ToString());
				},
				() =>
				{
					Trace.TraceInformation("Bolt shutting down.");
				});
		}

		/// <summary>
		/// If set, we will ack on receipt and NOT send the tuple for anchoring.
		/// </summary>
		public bool AutoAck { get; set; }

		/// <summary>
		/// If set, will use this instead of the default stream ID.
		/// </summary>
		public string OutputStreamId { get; set; }

		private Context Context { get; set; }

		/// <summary>
		/// IObservable for the input data - fed to the processor.
		/// </summary>
		private Subject<BoltInput<TIn>> Input { get; set; }

		/// <summary>
		/// Execute now just feeds the tuple to the observable (and, potentially, acks).
		/// </summary>
		public void Execute(SCPTuple tuple)
		{
			this.Input.OnNext(
				new BoltInput<TIn>()
				{
					Original = this.AutoAck ? null : tuple,
					Converted = this.ConvertInput(tuple)
				});
			if (this.AutoAck)
				this.Context.Ack(tuple);
		}

		/// <summary>
		/// Map schemas into the Context using DeclareComponentSchema.
		/// </summary>
		protected abstract void MapSchemas(Context context);

		/// <summary>
		/// Convert SCPTuple into the input type.
		/// </summary>
		protected abstract TIn ConvertInput(SCPTuple tuple);

		/// <summary>
		/// Convert output type into Values for Emitting.
		/// </summary>
		protected abstract Values ConvertOutput(TOut output);

		/// <summary>
		/// Process the input data from its observable into output data.
		/// </summary>
		protected abstract IObservable<BoltOutput<TOut>> ProcessInput(
			IObservable<BoltInput<TIn>> input);
	}
}