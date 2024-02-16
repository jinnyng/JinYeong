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
    List<float> v1 = new List<float>(); // x축 속도값 배열
    List<float> v2 = new List<float>(); // y축 속도값 배열
    List<float> v3 = new List<float>(); // z축 속도값 배열
    List<float> r1 = new List<float>(); // x축 위치값 배열
    List<float> r2 = new List<float>(); // y축 위치값 배열
    List<float> r3 = new List<float>(); // z축 위치값 배열


    public JsonData jsonData; // json 데이터 파일
    string path; // json 데이터 가져올 경로
    string data; // path로 가져온 데이터를 모두 담을 문자열

    private IProducer<Null, string> producer;

    async void Awake()
    {
        Debug.Log("Active Main func.");

        var config = new ProducerConfig { BootstrapServers = "211.193.31.69:9092" };

        producer = new ProducerBuilder<Null, string>(config)
            .SetErrorHandler((_, e) => Debug.LogError($"Error: {e.Reason}"))
            .SetStatisticsHandler((_, json) => Debug.Log($"Statistics: {json}")) // .comsume 단계에서 받아온 consumer 정보 json형식으로 콘솔에 표시                
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
            // 지정된 경로에 저장되어 있는 json 파일에서 데이터 값을 받아옴
            data = File.ReadAllText(path);
            // 받아온 문자열 데이터값을 오브젝트로 변환
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




