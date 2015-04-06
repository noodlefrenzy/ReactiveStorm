using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using System.Diagnostics;

namespace ReactiveStorm
{
	public class RxMovingAverageBolt : ReactiveBoltBase<int, double>
	{
		public static RxMovingAverageBolt Get(Context ctx, Dictionary<string, Object> parms)
		{
			return new RxMovingAverageBolt(ctx);
		}

		public RxMovingAverageBolt(Context context) : base(context)
		{
		}

		protected override int ConvertInput(SCPTuple tuple)
		{
			return tuple.GetInteger(0);
		}

		protected override Values ConvertOutput(double output)
		{
			return new Values(output);
		}

		protected override void MapSchemas(Context context)
		{
			Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
			inputSchema.Add("default", new List<Type>() { typeof(int) });

			Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
			outputSchema.Add("default", new List<Type>() { typeof(double) });

			context.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));
		}

		protected override IObservable<BoltOutput<double>> ProcessInput(
			IObservable<BoltInput<int>> input)
		{
			return input.Zip(input.Skip(1),
				(v1, v2) =>
				{
					var result = (v1.Converted + v2.Converted) / 2.0;
                    Trace.TraceInformation("{0} + {1} / 2 = {2}", v1.Converted, v2.Converted, result);
					return new BoltOutput<double>() { Result = result };
                });
		}
	}
}
