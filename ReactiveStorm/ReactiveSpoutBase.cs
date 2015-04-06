using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using System.Diagnostics;
using System.Reactive.Subjects;
using System.Threading.Tasks;

namespace ReactiveStorm
{
	public abstract class ReactiveSpoutBase<TOut> : ISCPSpout
	{
		// Child classes should implement this getter for building the topology.
		//public static ReactiveSpout Get(Context ctx, Dictionary<string, Object> parms)
		//{
		//	return new ReactiveSpout(ctx);
		//}

		public ReactiveSpoutBase(Context context)
		{
			this.Context = context;
			this.MapSchema(this.Context);
			this.Credits = new SemaphoreSlim(0);
			Task.Run(() =>
			{
				this.GenerateOutput().Subscribe(
					output =>
					{
						Trace.TraceInformation("Waiting for release to emit {0}", output);
						this.Credits.Wait();
						Trace.TraceInformation("Released, emitting {0}", output);
						this.Context.Emit(
							this.OutputStreamId ?? Constants.DEFAULT_STREAM_ID,
							this.ConvertOutput(output));
					},
					err =>
					{
						Trace.TraceError(err.ToString());
					},
					() =>
					{
						Trace.TraceInformation("Spout shutting down.");
						this.FinishedTransmitting = true;
						this.AckFailMessages.OnCompleted();
					});
			});
		}

		public bool FinishedTransmitting { get; set; }

		public string OutputStreamId { get; set; }

		protected Subject<Tuple<long, bool>> AckFailMessages = new Subject<Tuple<long, bool>>();

		private Context Context { get; set; }

		private SemaphoreSlim Credits { get; set; }

		protected abstract void MapSchema(Context context);

		protected abstract Values ConvertOutput(TOut output);

		protected abstract IObservable<TOut> GenerateOutput();

		public void NextTuple(Dictionary<string, object> parms)
		{
			Trace.TraceInformation("Releasing once");
			this.Credits.Release();
		}

		public void Ack(long seqId, Dictionary<string, Object> parms)
		{
			this.AckFailMessages.OnNext(Tuple.Create(seqId, true));
		}

		public void Fail(long seqId, Dictionary<string, Object> parms)
		{
			this.AckFailMessages.OnNext(Tuple.Create(seqId, false));
		}
	}
}