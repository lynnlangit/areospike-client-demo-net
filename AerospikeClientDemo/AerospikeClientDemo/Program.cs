using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AerospikeClientDemo
{
    public class Program
    {

        //Update the IP addresses to the values for YOUR Aerospike instance
        private static AerospikeClient connect()
        {
            Host[] hosts = new Host[] { new Host("192.168.1.11", 3000),
                new Host("192.168.1.23", 3000) };
            AerospikeClient client = new AerospikeClient(new ClientPolicy(), hosts);
            return client;
        }

        public static void Main(String[] args)
        {

            AerospikeClient client = connect();
            Policy policy = new Policy();
            WritePolicy writePolicy = new WritePolicy();
            BatchPolicy batchPolicy = new BatchPolicy();

            //NOTE: adjust the timeout value depending on your demo machine 
            writePolicy.timeout = 1000;
            Key key = new Key("test", "myset", "mykey");

            writeSingleValue(client, writePolicy, key);
            checkKeyExists(client, policy, key);
            addSingleValue(client, writePolicy, key);
            writeMultipleValues(client, writePolicy, key);
            writeValueWithTTL(client);

            readAllValuesForKey(client, policy, key);
            readSomeValuesForKey(client, policy, key);

            deleteValue(client, writePolicy, key);
            deleteRecord(client, writePolicy, key);

            addRecords(client, writePolicy);
            batchReadRecords(client, batchPolicy);

            multiOps(client, writePolicy, key);

            client.Close();
        }

        private static void writeSingleValue(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            Bin bin = new Bin("mybin", "myReadModifyWriteValue");
            client.Put(writePolicy, key, bin);
            Console.WriteLine("Wrote this new value (or bin): " + key);
            Console.WriteLine("");
        }

        private static void checkKeyExists(AerospikeClient client, Policy policy,
                Key key)
        {
            Console.WriteLine("Check a record exists");
            bool exists = client.Exists(policy, key);
            Console.WriteLine(key + " exists? " + exists);
            Console.WriteLine("");
        }

        private static void addSingleValue(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            Key newKey = new Key("test", "myAddSet", "myAddKey");
            Bin counter = new Bin("mybin", 1);
            client.Add(writePolicy, newKey, counter);
            Console.WriteLine("Wrote this additional value (or bin):  " + newKey);
            Console.WriteLine("");
        }

        private static void writeMultipleValues(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            Bin bin0 = new Bin("location", "Oslo");
            Bin bin1 = new Bin("name", "Lynn");
            Bin bin2 = new Bin("age", 42);
            client.Put(writePolicy, key, bin0, bin1, bin2);
            Console.WriteLine("Wrote these additional values:  " + key
                    + " " + bin0 + " " + bin1 + " " + bin2);
            Console.WriteLine("");
        }

        private static void writeValueWithTTL(AerospikeClient client)
        {
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.expiration = 2;

            Key ttlKey = new Key("test", "myTtlSet", "myTtlKey");
            Bin bin = new Bin("gender", "female");
            client.Put(writePolicy, ttlKey, bin);

            Policy policy = new Policy();
            checkKeyExists(client, policy, ttlKey);
            Console.WriteLine("sleeping for 4 seconds");
            try
            {
                Thread.Sleep(4000);
            }
            catch (ThreadInterruptedException e)
            {
                Console.WriteLine(e + "");
            }
            checkKeyExists(client, policy, ttlKey);
            Console.WriteLine("");
        }

        private static void readAllValuesForKey(AerospikeClient client, Policy policy, Key key)
        {
            Console.WriteLine("Read all bins of a record");
            Record record = client.Get(policy, key);
            Console.WriteLine("Read these values: " + record);
            Console.WriteLine("");
        }

        private static void readSomeValuesForKey(AerospikeClient client, Policy policy, Key key)
        {
            Console.WriteLine("Read specific values (or bins) of a record");
            Record record = client.Get(policy, key, "name", "age");
            Console.WriteLine("Read these values: " + record);
            Console.WriteLine("");
        }

        private static void deleteValue(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            Bin bin1 = Bin.AsNull("mybin");
            client.Put(writePolicy, key, bin1);
            Console.WriteLine("Deleted this value:  " + bin1);
            Console.WriteLine("");
        }

        private static void deleteRecord(AerospikeClient client,
                WritePolicy policy, Key key)
        {
            client.Delete(policy, key);
            checkKeyExists(client, policy, key);
            Console.WriteLine("Deleted this record: " + key);
            Console.WriteLine("");
        }

        private static void addRecords(AerospikeClient client,
                WritePolicy writePolicy)
        {
            int size = 1024;
            for (int i = 0; i < size; i++)
            {
                Key key = new Key("test", "myset", (i + 1));
                client.Put(writePolicy, key, new Bin("dots", i + " dots"));
            }
            Console.WriteLine("Added " + size + " Records");
            Console.WriteLine("");
        }

        private static void batchReadRecords(AerospikeClient client, BatchPolicy batchPolicy)
        {
            Console.WriteLine("Batch Reads");
            int size = 1024;
            Key[] keys = new Key[size];
            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = new Key("test", "myset", (i + 1));
            }
            Record[] records = client.Get(batchPolicy, keys);
            Console.WriteLine("Read " + records.Length + " records");
        }

        private static void multiOps(AerospikeClient client,
                WritePolicy writePolicy, Key key)
        {
            Console.WriteLine("Multiops");
            Bin bin1 = new Bin("optintbin", 7);
            Bin bin2 = new Bin("optstringbin", "string value");
            client.Put(writePolicy, key, bin1, bin2);
            Bin bin3 = new Bin(bin1.name, 4);
            Bin bin4 = new Bin(bin2.name, "new string");
            Record record = client.Operate(writePolicy, key, Operation.Add(bin3),
                    Operation.Put(bin4), Operation.Get());
            Console.WriteLine("Record: " + record);
        }

    }
}
