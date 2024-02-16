using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading;
using UnityEngine;

class KafkaProducer1 : MonoBehaviour
{
    public class JsonClass
    {
        public int No;
        public float r1;
        public float r2;
        public float r3;
        public float v1;
        public float v2;
        public float v3;

        public JsonClass() { }

        public JsonClass(bool isSet)
        {
            if (isSet)
            {
                No = 0;
                r1 = 0.0f;
                r2 = 0.0f;
                r3 = 0.0f;
                v1 = 0.0f;
                v2 = 0.0f;
            }
        }
    }
   
    [System.Serializable]
    public class _threadHandle
    {
        ProducerConfig config;

        public async void StartKafkaListener()
        {            
            Debug.Log("Kafka - Starting Thread..");
            try
            {
                config = new ProducerConfig
                {

                    BootstrapServers = "175.45.193.145:3000",
                };

                Debug.Log("Kafka - Created config");

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    CancellationTokenSource cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, e) =>
                    {
                        e.Cancel = true; // prevent the process from terminating.
                        cts.Cancel();
                    };
                    try
                    {
                        print("load json data");
                        var jsonData = LoadJsonFile<JsonClass>(Application.dataPath, "timeSatelite_sep");
                        print(jsonData);

                        for(int i = 0; i < 7; i++)
                        {
                            print("active produce");
                            var message = JsonConvert.SerializeObject(jsonData);
                            var dr = await producer.ProduceAsync("my-json-array-topic", new Message<Null, string> { Value = message });
                            Debug.Log($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                        }
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Debug.Log($"Delivery failed: {e.Error.Reason}");
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Log("Kafka - Received Expection: " + ex.Message + " trace: " + ex.StackTrace);
            }
        }

        T LoadJsonFile<T>(string loadPath, string fileName)
        {
            var filePath = $"{loadPath}/{fileName}.json";
            var _jsonData = System.IO.File.ReadAllText(filePath);
            return JsonConvert.DeserializeObject<T>(_jsonData);
        }
    }
    bool kafkaStarted = false;
    Thread kafkaThread;
    _threadHandle _handle;

    void Start()
    {
        StartKafkaThread();
    }
    void Update()
    {
        if (Input.GetKeyUp(KeyCode.LeftControl) && Input.GetKeyUp(KeyCode.C))
        {
            Debug.Log("Cancelling Kafka!");
            StopKafkaThread();
        }
    }

    void OnDisable()
    {
        StopKafkaThread();
    }
    void OnApplicationQuit()
    {
        StopKafkaThread();
    }

    public void StartKafkaThread()
    {
        if (kafkaStarted) return;

        print("startkafkathread");
        _handle = new _threadHandle();
        kafkaThread = new Thread(_handle.StartKafkaListener);

        kafkaThread.Start();
        kafkaStarted = true;
    }

    void StopKafkaThread()
    {
        if (kafkaStarted)
        {
            kafkaThread.Abort();
            kafkaThread.Join();
            kafkaStarted = false;
        }
    }
}
