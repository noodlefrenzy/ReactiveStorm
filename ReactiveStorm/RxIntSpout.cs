using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;

namespace ReactiveStorm
{
	public class RxIntSpout : ReactiveSpoutBase<int>
	{
		public static RxIntSpout Get(Context ctx, Dictionary<string, object> parms)
		{
			return new RxIntSpout(ctx, parms);
		}

		public RxIntSpout(Context context, Dictionary<string, object> parms) : base(context)
		{
			object val;
			if (parms == null || !parms.TryGetValue("MaxValue", out val))
			{
				this.MaxValue = 10;
			}
			else
			{
				this.MaxValue = int.Parse(val.ToString());
			}
		}

		private int MaxValue { get; set; }

		protected override Values ConvertOutput(int output)
		{
			return new Values(output);
		}

		protected override IObservable<int> GenerateOutput()
		{
			return Enumerable.Range(0, this.MaxValue + 1).ToObservable();
		}

		protected override void MapSchema(Context context)
		{
			Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
			outputSchema.Add("default", new List<Type>() { typeof(int) });

			context.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));
		}
	}
}
