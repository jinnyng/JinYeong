using LitJson;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using TMPro;
using UnityEngine;


public class VelocityTest : MonoBehaviour
{
    string path; // json ������ ������ ���
    string data; // path�� ������ �����͸� ��� ���� ���ڿ�
    public static VelocityTest instance; // �̱���

    List<float> v1 = new List<float>(); // x�� �ӵ��� �迭
    List<float> v2 = new List<float>(); // y�� �ӵ��� �迭
    List<float> v3 = new List<float>(); // z�� �ӵ��� �迭

    List<float> r1 = new List<float>(); // x�� ��ġ�� �迭
    List<float> r2 = new List<float>(); // y�� ��ġ�� �迭
    List<float> r3 = new List<float>(); // z�� ��ġ�� �迭
    public List<Vector3> orbit = new List<Vector3>(); // ������ ��� �̵� ������ ������ vector3 �迭

    public float dur = 10f;

    public GameObject satellite; // �̵���ų ��ü
    Vector3 satellitePos; // �̵���ų ��ü�� ��ġ��

    public LineRenderer line; // �˵�

    public int i = 0; // �迭���� �޾ƿ� ���� �ε��� �ѹ�
    int index = 1;

    JsonData jsonData; // json ������ ����

    public TextMeshProUGUI velocityXText;
    public TextMeshProUGUI velocityYText;
    public TextMeshProUGUI velocityZText;

    public TextMeshProUGUI positioinText;

    private void Awake()
    {
        // ���� ��ũ��Ʈ�� scene�� �Ѿ���� Destroy ���� �ʰ� ��
        if (instance == null)
        {
            instance = this;
        }
        else if (instance != this)
        {
            Destroy(instance.gameObject);
        }
        DontDestroyOnLoad(this.gameObject);

        // json ������ ����� ��� �Է�
        path = Application.dataPath + "/timeSatelite.json";
        // ������ ��ο� ����Ǿ� �ִ� json ���Ͽ��� ������ ���� �޾ƿ�
        data = File.ReadAllText(path);
        // �޾ƿ� ���ڿ� �����Ͱ��� ������Ʈ�� ��ȯ
        jsonData = JsonMapper.ToObject(data);

        // json ���Ͽ��� ��ġ ������ �޾ƿ���
        PositionInJson();
        // json���Ͽ��� �ӵ� ������ �޾ƿ���
        VelocityInJson();

        // ȭ�� fps�� 60���� ����
        Application.targetFrameRate = 60;
    }
    void Start()
    {
        // �������� ĵ���� ��Ȱ��ȭ
        infoCanvas.SetActive(false);
        backgroundImage.SetActive(false);

        // ��ü�� ��ġ�� �޾ƿ� ��ġ ������ ��ǥ�� ����
        satellite.transform.position = new Vector3(r1[0], r2[0], r3[0]);
        // ���η����� �ʱ�ȭ
        line.positionCount = 0;
        // �޾ƿ� ��ġ������ ���, ���η������� �˵� �׸���
        MakeOrbitwithLineRenderer(r1,r2,r3);

        StartCoroutine(Move(dur));
    }

    void Update()
    {
        if (r1.Count > 0)
        {
            HitSatellite();

            ShowSatelliteInfo(i);
        }
    }



    IEnumerator Move(float duration)
    {
        var runTime = 0.0f;
        i++;
        //double SpeedwithPosition = Math.Sqrt(Math.Pow(satellite.transform.position.x - r1[i], 2) + Math.Pow(satellite.transform.position.y - r2[i], 2) + Math.Pow(satellite.transform.position.z - r3[i], 2));
        double SpeedwithVelocity = Math.Sqrt(Math.Pow(v1[i], 2) + Math.Pow(v2[i], 2) + Math.Pow(v3[i], 2));
        /*print(string.Format("(time){0} * (PosSpeed){1} / (duration)10f = (total){2}", Time.deltaTime, (float)SpeedwithPosition, (Time.deltaTime * (float)SpeedwithPosition / 10f)));
        print(string.Format("(time){0} * (VelSpeed){1} / (duration)10f = (total){2}", Time.deltaTime, (float)SpeedwithPosition, (Time.deltaTime * (float)SpeedwithVelocity / 10f)));*/
        while (runTime < duration)
        {
            runTime += Time.deltaTime;
            satellite.transform.position = Vector3.MoveTowards(satellite.transform.position, new Vector3(r1[i], r2[i], r3[i]), Time.deltaTime * (float)SpeedwithVelocity);

            yield return null;
        }
        orbit.Add(satellite.transform.position);
        satellitePos = satellite.transform.position;
        print(string.Format("{0}�� ���",i*10));
        StartCoroutine(Move(dur));
    }
    void ShowSatelliteInfo(int i)
    {
        positioinText.text = string.Format("Next Point : \n x = {0}, y = {1}, z = {2} " +
            "\n CurrentPosition :\n x = {3}, y = {4}, z = {5}", r1[i], r2[i], r3[i], satellite.transform.position.x, satellite.transform.position.y, satellite.transform.position.z);

        float speedx = (r1[i] - satellitePos.x) / 10f;
        float speedy = (r2[i] - satellitePos.y) / 10f;
        float speedz = (r3[i] - satellitePos.z) / 10f;

        velocityXText.text = string.Format("X : {0}, {1}", v1[i], speedx);
        velocityYText.text = string.Format("Y : {0}, {1}", v2[i], speedy);
        velocityZText.text = string.Format("Z : {0}, {1}", v3[i], speedz);
    }

    void VelocityInJson()
    {
        for (int i = 0; i < jsonData.Count; i++)
        {
            v1.Add(ParsingJsonQuest(i, "v1", jsonData));
            v2.Add(ParsingJsonQuest(i, "v2", jsonData));
            v3.Add(ParsingJsonQuest(i, "v3", jsonData));
        }
    }

    void PositionInJson()
    {
        for (int i = 0; i < jsonData.Count; i++)
        {
            r1.Add(ParsingJsonQuest(i, "r1", jsonData));
            r2.Add(ParsingJsonQuest(i, "r2", jsonData));
            r3.Add(ParsingJsonQuest(i, "r3", jsonData));
        }
    } 

    public float ParsingJsonQuest(int _i, string name, JsonData data) // json ������ ���Ͽ��� �����Ͱ��� string ���·� �Ľ��ϴ� float�� �Լ�
    {
        // string���� ����� r1 ������ _i��°�� �ִ� name Ű�� �������� ������
        string v1 = data[_i][name].ToString();
        // string �������� ������ ������ ���� float �������� ��ȯ�Ͽ� ��ȯ��
        return float.Parse(v1);
    }

    public void MakeOrbitwithLineRenderer(List<float> x, List<float> y, List<float> z)
    {
        // ���η����� ������Ʈ ���� Positions �迭 Size ����
        line.positionCount = x.Count;        

        // �޾ƿ� ��ġ ������ �迭�� ���̸�ŭ �ݺ�
        for (int i = 0; i < line.positionCount; i++)
        { 
            // Positions �迭�� json���� �޾ƿ� ��ġ������ ��ǥ�� �߰�
            line.SetPosition(i, new Vector3(x[i], y[i], z[i]));
            // ���η����� ������Ʈ Ȱ��ȭ
            line.enabled = true;
        }
    }
    public TextMeshProUGUI rayDis;
    public TextMeshProUGUI objName;

    public GameObject infoCanvas;
    public GameObject backgroundImage;
    void HitSatellite()
    {
        RaycastHit hit;
        Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);

        if (Physics.Raycast(ray, out hit))
        {
            if (hit.transform.tag == "Satellite")
            {
                if (Input.GetMouseButtonDown(0))
                {
                    if (infoCanvas != null)
                    {
                        infoCanvas.SetActive(true);
                        backgroundImage.SetActive(true);
                    }
                }
            }
            else
            {
                DisableUI();
            }

            float hitDistance = hit.distance;

            rayDis.text = "RayDistance : " + hitDistance;
            objName.text = "Name : " + hit.transform.name;
        }
        else
        {
            DisableUI();
        }
    }
    void DisableUI()
    {
        // UI Canvas�� ��Ȱ��ȭ
        if (Input.GetMouseButtonDown(0))
        {
            infoCanvas.SetActive(false);
            backgroundImage.SetActive(false);
        }
    }
}
