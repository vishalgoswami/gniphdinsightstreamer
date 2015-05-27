using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TopHashtagMapReducer
{
    class HashtagCounter
    {
        public static ArrayList Process(string inputLine)
        {
            var arrayList = new ArrayList();
            try
            {
                JObject jObject = JObject.Parse(inputLine);
            }
            catch (Exception ex) {
                arrayList = null;
            }
            return arrayList;
        }
    }
}
