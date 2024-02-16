using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TMPro;
using UnityEngine;

class KafkaProducer : MonoBehaviour
{
    public TextMeshProUGUI timeText;
    public static class JsonHelper
    {
        public static T[] FromJson<T>(string json)
        {
            Wrapper<T> wrapper = JsonUtility.FromJson<Wrapper<T>>(json);
            return wrapper.Items;
        }

        public static string ToJson<T>(T[] array)
        {
            Wrapper<T> wrapper = new Wrapper<T>();
            wrapper.Items = array;
            return JsonUtility.ToJson(wrapper);
        }

        public static string ToJson<T>(T[] array, bool prettyPrint)
        {
            Wrapper<T> wrapper = new Wrapper<T>();
            wrapper.Items = array;
            return JsonUtility.ToJson(wrapper, prettyPrint);
        }

        [Serializable]
        private class Wrapper<T>
        {
            public T[] Items;
        }
    }
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
    public class JsonArrayClass
    {
        public List<JsonClass> satelliteArray;

        public JsonArrayClass() { }
        public JsonArrayClass(string message)
        {

            //satelliteArray.Add();
        }
    }
    /*private void Start()
    {
       
    JsonClass jsonClass = new JsonClass(true);
        string jsonData = ObjectToJson(jsonClass);
        Debug.Log(jsonData);
    
        JsonArrayClass jsonArray = new JsonArrayClass();

        
    }

    private IProducer<string, string> producer;

    async void Awake()
    {
        Debug.Log("Active Main func.");

        var config = new ProducerConfig { BootstrapServers = "175.45.193.145:3000" };

        using (var schemaRegistry = new CachedSchemaRegistryClient(new schemaRegistyConfig { Url = schemaRegistryUrl }))
            producer = new ProducerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => Debug.LogError($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Debug.Log($"Statistics: {json}")) // .comsume 단계에서 받아온 consumer 정보 json형식으로 콘솔에 표시                
                .Build();
        await SendDataToKafka();
    }
    public string message;

    async Task SendDataToKafka()
    {
        JsonClass jsonClass = new JsonClass();

        var jsonClass2 = LoadJsonFile<JsonClass>(Application.dataPath, "timeSatelite_sep");
        Debug.Log(jsonClass2.r1);

        for (int i = 0; i < 7; i++)
        {
            print(string.Format("{0}, {1}, {2}, {3}, {4}, {5}, {6}", jsonClass2.No, jsonClass2.r1, jsonClass2.r2, jsonClass2.r3, jsonClass2.v1, jsonClass2.v2, jsonClass2.v3));
        }


        var topicName = "my-json-array-topic";
        try
        {
            Debug.Log("\n-----------------------------------------------------------------------");
            Debug.Log($"Producer {producer.Name} producing on topic {topicName}.");

            Debug.Log("-----------------------------------------------------------------------");
            Debug.Log("To create a kafka message with UTF-8 encoded key and value:");
            //Debug.Log("> key value<Enter>");
            Debug.Log("To create a kafka message with a null key and UTF-8 encoded value:");
            //Debug.Log("> value<enter>");
            Debug.Log("Ctrl-C to quit.\n");

            var cancelled = false;
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cancelled = true;
            };
            while (!cancelled)
            {
                string key = null;
                string val;
                for (int i = 0; i < 7; i++)
                {
                    val = JsonUtility.ToJson(new
                    {
                        No = jsonClass2.No,
                        r1 = jsonClass2.r1, 
                        r2 = jsonClass2.r2,
                        r3 = jsonClass2.r3,
                        v1 = jsonClass2.v1,
                        v2 = jsonClass2.v2,
                        v3 = jsonClass2.v3

                    });
                    message = val;
                }

                try
                {
                    // Note: Awaiting the asynchronous produce request below prevents flow of execution
                    // from proceeding until the acknowledgement from the broker is received (at the 
                    // expense of low throughput).
                    var deliveryReport = await producer.ProduceAsync(
                        topicName, new Message<string, string> { Key = key, Value = message });

                    Debug.Log($"delivered to: {deliveryReport.TopicPartitionOffset}");
                }
                catch (ProduceException<string, string> e)
                {
                    Debug.LogError($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
                print("end"); 
            }
            // Since we are producing synchronously, at this point there will be no messages
            // in-flight and no delivery reports waiting to be acknowledged, so there is no
            // need to call producer.Flush before disposing the producer.
        }
        catch (OperationCanceledException e)
        {
            Debug.Log("Process cancelled ..." + e);
        }
    }*/

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
                    BootstrapServers = "211.193.31.69:9092",
                };

                Debug.Log("Kafka - Created config");

                using (var producer = new ProducerBuilder<Null, string>(config)
.SetLogHandler(IProducer<Null, string>,"">                     .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                    .Build())
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

                        for (int i = 0; i < 7; i++)
                        {
                            var message = JsonConvert.SerializeObject(jsonData);
                            print(message);
                            string val = message;

                            // split line if both key and value specified.
                            int index = message.IndexOf(" ");
                            if (index != -1)
                            {
                                //key = message.Substring(0, index);
                                val = message.Substring(index + 1);
                            }
                            print(val);

                            var dr = await producer.ProduceAsync("quickstart-events", new Message<Null, string> { Value = "Hello World!" });
                            Debug.Log($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                        }
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Debug.LogError($"Delivery failed: {e.Error.Reason}");
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.LogError("Kafka - Received Expection: " + ex.Message + " trace: " + ex.StackTrace);
            }
        }

        T LoadJsonFile<T>(string loadPath, string fileName)
        {
            /*FileStream fileStream = new FileStream(string.Format("{0}/{1}.json", loadPath, fileName), FileMode.Open);
            byte[] data = new byte[fileStream.Length];
            fileStream.Read(data, 0, data.Length);
            fileStream.Close();
            string _jsonData = Encoding.UTF8.GetString(data);
            print(_jsonData);*/

            var filePath = $"{loadPath}/{fileName}.json";
            var _jsonData = System.IO.File.ReadAllText(filePath);
            return JsonConvert.DeserializeObject<T>(_jsonData);
        }
        string GetData(string text)
        {
            JsonClass jsonClass = new JsonClass();

            var jsonClass2 = LoadJsonFile<JsonClass>(Application.dataPath, "timeSatelite_sep");
            for (int i = 0; i < 7; i++)
            {
                text = JsonUtility.ToJson(new
                {
                    No = jsonClass.No,
                    r1 = jsonClass.r1,
                    r2 = jsonClass.r2,
                    r3 = jsonClass.r3,
                    v1 = jsonClass.v1,
                    v2 = jsonClass.v2,
                    v3 = jsonClass.v3
                });
            }
            return text;
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
        timeText.text = Time.time.ToString();
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

    /*void CreateJsonFile(string filePath, string fileName, string jsonData)
    {
        FileStream fileStream = new FileStream(string.Format("{0}/{1}.json", filePath, fileName), FileMode.Create);
        byte[] data = Encoding.UTF8.GetBytes(jsonData);
        fileStream.Write(data, 0, data.Length);
        fileStream.Close();
    }

    
    string ObjectToJson(object obj)
    { return JsonConvert.SerializeObject(obj); }
    T JsonToOject<T>(string jsonData)
    { return JsonConvert.DeserializeObject<T>(jsonData); }*/
}
