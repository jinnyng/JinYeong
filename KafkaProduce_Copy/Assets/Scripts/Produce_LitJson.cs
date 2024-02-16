using Confluent.Kafka;
using LitJson;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using UnityEngine;

class Produce_LitJson : MonoBehaviour
{
    List<int> no = new List<int>();
    List<float> v1 = new List<float>(); // x�� �ӵ��� �迭
    List<float> v2 = new List<float>(); // y�� �ӵ��� �迭
    List<float> v3 = new List<float>(); // z�� �ӵ��� �迭
    List<float> r1 = new List<float>(); // x�� ��ġ�� �迭
    List<float> r2 = new List<float>(); // y�� ��ġ�� �迭
    List<float> r3 = new List<float>(); // z�� ��ġ�� �迭


    public JsonData jsonData; // json ������ ����
    string path; // json ������ ������ ���
    string data; // path�� ������ �����͸� ��� ���� ���ڿ�

    private IProducer<Null, string> producer;

    async void Awake()
    {
        Debug.Log("Active Main func.");

        var config = new ProducerConfig { BootstrapServers = "211.193.31.69:9092" };

        producer = new ProducerBuilder<Null, string>(config)
            .SetErrorHandler((_, e) => Debug.LogError($"Error: {e.Reason}"))
            .SetStatisticsHandler((_, json) => Debug.Log($"Statistics: {json}")) // .comsume �ܰ迡�� �޾ƿ� consumer ���� json�������� �ֿܼ� ǥ��                
            .Build();
        LoadMapData();
        await SendDataToKafka();
    }
    public string message = "Hello World!";

    async Task SendDataToKafka()
    {
        var topicName = "quickstart-events";
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
                try
                {
                    for (int i = 0; i < no.Count; i++)
                    {

                    }
                    // Note: Awaiting the asynchronous produce request below prevents flow of execution
                    // from proceeding until the acknowledgement from the broker is received (at the 
                    // expense of low throughput).
                    var deliveryReport = await producer.ProduceAsync(
                        topicName, new Message<Null, string> { Value = message });

                    Debug.Log($"delivered to: {deliveryReport.TopicPartitionOffset}");
                }
                catch (ProduceException<string, string> e)
                {
                    Debug.Log($"failed to deliver message: {e.Message} [{e.Error.Code}]");
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
    }
    public void LoadMapData()
    {
        if (jsonData == null)
        {
            path = Application.dataPath + "/Resources/Json/timeSatelite.json";
            // ������ ��ο� ����Ǿ� �ִ� json ���Ͽ��� ������ ���� �޾ƿ�
            data = File.ReadAllText(path);
            // �޾ƿ� ���ڿ� �����Ͱ��� ������Ʈ�� ��ȯ
            /*TextAsset jdata = Resources.Load<TextAsset>("Json/timeSatelite");
            string mapText = jdata.text;*/
            jsonData = JsonMapper.ToObject(data);
            ParsingJsonData(jsonData);
        }
        else
        {
            Debug.Log(jsonData.ToString());
        }
    }
    private void ParsingJsonData(JsonData jsonData)
    {
        for (int i = 0; i < jsonData.Count; i++)
        {
            no.Add(int.Parse(jsonData[i]["No."].ToString()));
            v1.Add(float.Parse(jsonData[i]["v1"].ToString()));
            v2.Add(float.Parse(jsonData[i]["v2"].ToString()));
            v3.Add(float.Parse(jsonData[i]["v3"].ToString()));
            r1.Add(float.Parse(jsonData[i]["r1"].ToString()));
            r2.Add(float.Parse(jsonData[i]["r2"].ToString()));
            r3.Add(float.Parse(jsonData[i]["r3"].ToString()));
        }
    }
    private void OnDestroy()
    {
        Debug.Log("Ondestroy");
        producer.Dispose();
    }
}




