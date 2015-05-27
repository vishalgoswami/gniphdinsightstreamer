using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TopHashtagMapReducer
{
    class Program
    {
        public class TopHashtagMapper : MapperBase
        {
            public override void Map(string inputLine, MapperContext context)
            {
                ArrayList hashTags = HashtagCounter.Process(inputLine);
                context.EmitKeyValue("vishal", "1");
                /*if (hashTags != null)
                {
                    foreach (string hashTag in hashTags) {
                        context.EmitKeyValue(hashTag,"1");
                    }
                }*/
            }
        }

        public class TopHashtagReducer : ReducerCombinerBase
        {
            public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
            {
 	            Int32 totalValue = 0;
                foreach (string value in values)
                {
                    totalValue += Int32.Parse(value);
                }
                if (totalValue > 10)
                    context.EmitKeyValue(key,totalValue.ToString());
            }
        }

        public class TopHashtagProcessingJob : HadoopJob<TopHashtagMapper>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                var config = new HadoopJobConfiguration();
                config.InputPath = "input";
                config.OutputFolder = "output";
                return config;
            }
        }
        static void Main(string[] args)
        {
            /*var hadoop = Hadoop.Connect(new Uri("https://testhdinsightclusterfhtrue.azurehdinsight.net"), "admin", "vishal", "P@ssw0rd!", 
                "teststorageaccountfh1.blob.core.windows.net",
                "ramrGxXNbp7tdNnL8mF+Se2cwMdh+c4pSsKCdYcoYsvDrinrZw3aW7YVDzxoneZM6pBMJUGsM+8lxIeNkyN+AA==", 
                "testhdinsightclusterfhtrue", 
                false);*/
            var hadoop = Hadoop.Connect();
            var result = hadoop.MapReduceJob.ExecuteJob<TopHashtagProcessingJob>();
        }
    }
}
