using Aerospike.Client;
using System;
using System.Threading;

namespace AerospikeClientDemo
{
    public class Program
    {
        //Update the IP addresses to the values for YOUR Aerospike instance
        private static AerospikeClient Connect()
        {
            var hosts = new[] { new Host("192.168.1.11", 3000),
                new Host("192.168.1.23", 3000) };
            var client = new AerospikeClient(new ClientPolicy(), hosts);
            return client;
        }

        public static void Main(string[] args)
        {
            var client = Connect();
            var policy = new Policy();
            var writePolicy = new WritePolicy();
            var batchPolicy = new BatchPolicy();

            //NOTE: adjust the timeout value depending on your demo machine
            writePolicy.timeout = 1000;
            var key = new Key("test", "myset", "mykey");

            WriteSingleValue(client, writePolicy, key);
            CheckKeyExists(client, policy, key);
            AddSingleValue(client, writePolicy);
            WriteMultipleValues(client, writePolicy, key);
            WriteValueWithTtl(client);

            ReadAllValuesForKey(client, policy, key);
            ReadSomeValuesForKey(client, policy, key);

            DeleteValue(client, writePolicy, key);
            DeleteRecord(client, writePolicy, key);

            AddRecords(client, writePolicy);
            BatchReadRecords(client, batchPolicy);

            MultiOps(client, writePolicy, key);

            client.Close();
        }

        private static void WriteSingleValue(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            var bin = new Bin("mybin", "myReadModifyWriteValue");
            client.Put(writePolicy, key, bin);
            Console.WriteLine("Wrote this new value (or bin): " + key);
            Console.WriteLine("");
        }

        private static void CheckKeyExists(AerospikeClient client, Policy policy,
                Key key)
        {
            Console.WriteLine("Check a record exists");
            var exists = client.Exists(policy, key);
            Console.WriteLine(key + " exists? " + exists);
            Console.WriteLine("");
        }

        private static void AddSingleValue(AerospikeClient client,
                WritePolicy writePolicy)
        {
            var newKey = new Key("test", "myAddSet", "myAddKey");
            var counter = new Bin("mybin", 1);
            client.Add(writePolicy, newKey, counter);
            Console.WriteLine("Wrote this additional value (or bin):  " + newKey);
            Console.WriteLine("");
        }

        private static void WriteMultipleValues(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            var bin0 = new Bin("location", "Oslo");
            var bin1 = new Bin("name", "Lynn");
            var bin2 = new Bin("age", 42);
            client.Put(writePolicy, key, bin0, bin1, bin2);
            Console.WriteLine("Wrote these additional values:  " + key
                    + " " + bin0 + " " + bin1 + " " + bin2);
            Console.WriteLine("");
        }

        private static void WriteValueWithTtl(AerospikeClient client)
        {
            var writePolicy = new WritePolicy { expiration = 2 };

            var ttlKey = new Key("test", "myTtlSet", "myTtlKey");
            var bin = new Bin("gender", "female");
            client.Put(writePolicy, ttlKey, bin);

            var policy = new Policy();
            CheckKeyExists(client, policy, ttlKey);
            Console.WriteLine("sleeping for 4 seconds");
            try
            {
                Thread.Sleep(4000);
            }
            catch (ThreadInterruptedException e)
            {
                Console.WriteLine(e + "");
            }
            CheckKeyExists(client, policy, ttlKey);
            Console.WriteLine("");
        }

        private static void ReadAllValuesForKey(AerospikeClient client, Policy policy, Key key)
        {
            Console.WriteLine("Read all bins of a record");
            var record = client.Get(policy, key);
            Console.WriteLine("Read these values: " + record);
            Console.WriteLine("");
        }

        private static void ReadSomeValuesForKey(AerospikeClient client, Policy policy, Key key)
        {
            Console.WriteLine("Read specific values (or bins) of a record");
            var record = client.Get(policy, key, "name", "age");
            Console.WriteLine("Read these values: " + record);
            Console.WriteLine("");
        }

        private static void DeleteValue(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            var bin1 = Bin.AsNull("mybin");
            client.Put(writePolicy, key, bin1);
            Console.WriteLine("Deleted this value:  " + bin1);
            Console.WriteLine("");
        }

        private static void DeleteRecord(AerospikeClient client,
                WritePolicy policy, Key key)
        {
            client.Delete(policy, key);
            CheckKeyExists(client, policy, key);
            Console.WriteLine("Deleted this record: " + key);
            Console.WriteLine("");
        }

        private static void AddRecords(AerospikeClient client,
                WritePolicy writePolicy)
        {
            const int size = 1024;
            for (var i = 0; i < size; i++)
            {
                var key = new Key("test", "myset", (i + 1));
                client.Put(writePolicy, key, new Bin("dots", i + " dots"));
            }
            Console.WriteLine("Added " + size + " Records");
            Console.WriteLine("");
        }

        private static void BatchReadRecords(AerospikeClient client, BatchPolicy batchPolicy)
        {
            Console.WriteLine("Batch Reads");
            const int size = 1024;
            var keys = new Key[size];
            for (var i = 0; i < keys.Length; i++)
            {
                keys[i] = new Key("test", "myset", (i + 1));
            }
            var records = client.Get(batchPolicy, keys);
            Console.WriteLine("Read " + records.Length + " records");
        }

        private static void MultiOps(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            Console.WriteLine("Multiops");
            var bin1 = new Bin("optintbin", 7);
            var bin2 = new Bin("optstringbin", "string value");
            client.Put(writePolicy, key, bin1, bin2);
            var bin3 = new Bin(bin1.name, 4);
            var bin4 = new Bin(bin2.name, "new string");
            var record = client.Operate(writePolicy, key, Operation.Add(bin3),
                    Operation.Put(bin4), Operation.Get());
            Console.WriteLine("Record: " + record);
        }
    }
}