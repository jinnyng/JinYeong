using Confluent.Kafka;
using LitJson;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

//Serializable �Ӽ� = Json serializer�� ����
[System.Serializable]

//json ����ȭ(����ȭ�� json ���� ���)
public class MyData // �����Ϸ��� ���� Ŭ���� �Ǵ� ���� ����
{
    private int _no;
    public int No
    {
        get { return _no; }
        set { _no = value; }
    }
    public float r1;
    public float r2;
    public float r3;
    public float v1;
    public float v2;
    public float v3;
}

[System.Serializable]
public class MyDataArray
{
    public MyData[] Satelite;
}

class Produce : MonoBehaviour
{
    //[Serializable]
    Thread kafkaThread;
    threadHandle _handle;
    static string filePath;
    string jsonString;

    string path; // json ������ ������ ���
    string data; // path�� ������ �����͸� ��� ���� ���ڿ�
    JsonData jsonData; // json ������ ����

    private void Start()
    {
        // List<MyData> jsonData = new List<MyData>();
        MyData jsonData = new MyData(); // Ŭ���� �ν��Ͻ�
        /* jsonData.No = 1;
         * jsonData.r1 = 5062.47;
         * .
         * .
         * .
        */

        MyDataArray myObj = new MyDataArray(); //Satelite
        MyData myDataObj = new MyData();//r1~v3

        myDataObj.r1 = 30.5f;
        filePath = Path.Combine(Application.dataPath, "timeSatelite.json");
        //jsonfile = Resources.Load<TextAsset>("timeSatelite");
        string jsonFile = File.ReadAllText(filePath);
        print(jsonFile);
        string jsonArray = JsonUtility.ToJson(jsonFile, true);

        print(myObj.Satelite[0].r1);
/*        for (int i = 0; i < jsonFile.Length; i++)
        {

        }*/

        if (jsonFile != null)
        {
            jsonData = JsonUtility.FromJson<MyData>(jsonArray);

            StartProduce();
        }
        else
        {
            Debug.LogError("No file available.");
        }

    }
    public static MyDataArray CreateFromJson(string jsonfile)
    {
        return JsonUtility.FromJson<MyDataArray>(jsonfile);
    }
    void StartProduce()
    {
        Debug.Log("Thread get started..");

        _handle = new threadHandle();
        kafkaThread = new Thread(() => _handle.Main("175.45.193.145:3000", "my-group"));

        Debug.Log("Start");
        kafkaThread.Start();
    }
    public class threadHandle
    {

        public async void Main(string brokerList, string topicName)
        {
            Debug.Log("Active Main func.");

            var config = new ProducerConfig { BootstrapServers = brokerList };

            var producer = new ProducerBuilder<string, string>(config).Build();
            try
            {
                Debug.Log("\n-----------------------------------------------------------------------");
                Debug.Log($"Producer {producer.Name} producing on topic {topicName}.");
                Debug.Log("-----------------------------------------------------------------------");
                Debug.Log("To create a kafka message with UTF-8 encoded key and value:");
                Debug.Log("> key value<Enter>");
                Debug.Log("To create a kafka message with a null key and UTF-8 encoded value:");
                Debug.Log("> value<enter>");
                Debug.Log("Ctrl-C to quit.\n");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Debug.Log("> ");

                    string text;
                    try
                    {
                        Debug.Log("Start parsing json");


                        //text = JsonUtility.ToJson<MyData>(.ToString();
                        text = Console.ReadLine();
                    }
                    catch (IOException)
                    {
                        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
                        Debug.Log("Connect Json failed.");
                        break;
                    }
                    if (text == null)
                    {
                        // Console returned null before 
                        // the CancelKeyPress was treated
                        break;
                    }
                    MyData myData = new MyData();
                    string key = null;
                    //string val = CreateFromJson(MyDataArray);
                    string val = text;

                    // split line if both key and value specified.
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = text.Substring(0, index);
                        val = text.Substring(index + 1);
                    }
                    try
                    {
                        // Note: Awaiting the asynchronous produce request below prevents flow of execution
                        // from proceeding until the acknowledgement from the broker is received (at the 
                        // expense of low throughput).
                        var deliveryReport = await producer.ProduceAsync(
                            topicName, new Message<string, string> { Key = key, Value = val });

                        Debug.Log($"delivered to: {deliveryReport.TopicPartitionOffset}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Debug.Log($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                }
                // Since we are producing synchronously, at this point there will be no messages
                // in-flight and no delivery reports waiting to be acknowledged, so there is no
                // need to call producer.Flush before disposing the producer.
            }
            catch (OperationCanceledException e)
            {
                Debug.Log("Process cancelled ..." + e);
            }
        }
        /*public IEnumerator AsyncKafkaProduce()
        {
            Task.Run(() => Main("175.45.193.145:3000", "my-group"));
            yield return KafkaAsyncOperation();
        }*/



        async Task KafkaAsyncOperation()
        {
            await Task.Delay(1000);
            Debug.Log("Async operation completed.");
        }

    }

    public float ParsingJsonQuest(int i, string name, JsonData data) // json ������ ���Ͽ��� �����Ͱ��� string ���·� �Ľ��ϴ� float�� �Լ�
    {
        // string���� ����� r1 ������ _i��°�� �ִ� name Ű�� �������� ������
        string message = data[i][name].ToString();
        // string �������� ������ ������ ���� float �������� ��ȯ�Ͽ� ��ȯ��
        return float.Parse(message);
    }
}


