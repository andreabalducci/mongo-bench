using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace mongo_bench
{
    internal class Data
    {
        public ObjectId Id { get; set; }
        public string Text { get; set; }
    }

    public class Runner
    {
        private int threads;
        private int completed = 0;
        private ManualResetEventSlim _start = new ManualResetEventSlim();
        private ManualResetEventSlim _stop = new ManualResetEventSlim();
        private Stopwatch _stopwatch;

        public Runner(int threads)
        {
            this.threads = threads;
            var client = new MongoClient();
            var db = client.GetServer().GetDatabase("mongo-bench",WriteConcern.Acknowledged);
            db.Drop();

            for (int c = 1; c <= threads; c++)
            {
                var collection = db.GetCollection<Data>("data-" + c);
                var t = new Thread(Run);
                t.Start(collection);
            }
        }

        private void Run(object obj)
        {
            var collection = (MongoCollection<Data>)obj;
            _start.Wait();

            var data = new Data()
            {
                Text = "0123456789qwertyuiopasdfghjklzxcvbnm"
            };

            for (int c = 0; c < 10000; c++)
            {
                data.Id = ObjectId.GenerateNewId();
                collection.Insert(data);
            }

            if (Interlocked.Increment(ref completed) == this.threads)
            {
                _stopwatch.Stop();
                _stop.Set();
            }
        }

        public void Start()
        {
            _stopwatch = new Stopwatch();
            Console.Write("Starting {0:D2} threads..", this.threads);
            _stopwatch.Start();
            _start.Set();
            _stop.Wait();
            Console.WriteLine("..time: {0:D10} ms", _stopwatch.ElapsedMilliseconds);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Mongo driver: {0}", typeof(MongoServer).Assembly.FullName);
            var runs = new[] {1, 2, 5, 7, 10};

            foreach (var run in runs)
            {
                var runner = new Runner(run);
                runner.Start();             
            }
        }
    }
}
