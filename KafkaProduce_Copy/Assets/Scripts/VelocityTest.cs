using LitJson;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using TMPro;
using UnityEngine;


public class VelocityTest : MonoBehaviour
{
    string path; // json 데이터 가져올 경로
    string data; // path로 가져온 데이터를 모두 담을 문자열
    public static VelocityTest instance; // 싱글톤

    List<float> v1 = new List<float>(); // x축 속도값 배열
    List<float> v2 = new List<float>(); // y축 속도값 배열
    List<float> v3 = new List<float>(); // z축 속도값 배열

    List<float> r1 = new List<float>(); // x축 위치값 배열
    List<float> r2 = new List<float>(); // y축 위치값 배열
    List<float> r3 = new List<float>(); // z축 위치값 배열
    public List<Vector3> orbit = new List<Vector3>(); // 데이터 기반 이동 지점을 저장할 vector3 배열

    public float dur = 10f;

    public GameObject satellite; // 이동시킬 객체
    Vector3 satellitePos; // 이동시킬 객체의 위치값

    public LineRenderer line; // 궤도

    public int i = 0; // 배열에서 받아올 값의 인덱스 넘버
    int index = 1;

    JsonData jsonData; // json 데이터 파일

    public TextMeshProUGUI velocityXText;
    public TextMeshProUGUI velocityYText;
    public TextMeshProUGUI velocityZText;

    public TextMeshProUGUI positioinText;

    private void Awake()
    {
        // 현재 스크립트가 scene을 넘어가더라도 Destroy 되지 않게 함
        if (instance == null)
        {
            instance = this;
        }
        else if (instance != this)
        {
            Destroy(instance.gameObject);
        }
        DontDestroyOnLoad(this.gameObject);

        // json 파일이 저장된 경로 입력
        path = Application.dataPath + "/timeSatelite.json";
        // 지정된 경로에 저장되어 있는 json 파일에서 데이터 값을 받아옴
        data = File.ReadAllText(path);
        // 받아온 문자열 데이터값을 오브젝트로 변환
        jsonData = JsonMapper.ToObject(data);

        // json 파일에서 위치 데이터 받아오기
        PositionInJson();
        // json파일에서 속도 데이터 받아오기
        VelocityInJson();

        // 화면 fps를 60으로 설정
        Application.targetFrameRate = 60;
    }
    void Start()
    {
        // 위성정보 캔버스 비활성화
        infoCanvas.SetActive(false);
        backgroundImage.SetActive(false);

        // 객체의 위치를 받아온 위치 데이터 좌표로 설정
        satellite.transform.position = new Vector3(r1[0], r2[0], r3[0]);
        // 라인렌더러 초기화
        line.positionCount = 0;
        // 받아온 위치데이터 기반, 라인렌더러로 궤도 그리기
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
        print(string.Format("{0}초 경과",i*10));
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

    public float ParsingJsonQuest(int _i, string name, JsonData data) // json 데이터 파일에서 데이터값을 string 형태로 파싱하는 float형 함수
    {
        // string으로 선언된 r1 변수에 _i번째에 있는 name 키의 벨류값을 가져옴
        string v1 = data[_i][name].ToString();
        // string 형식으로 가져온 데이터 값을 float 형식으로 변환하여 반환함
        return float.Parse(v1);
    }

    public void MakeOrbitwithLineRenderer(List<float> x, List<float> y, List<float> z)
    {
        // 라인렌더러 컴포넌트 내의 Positions 배열 Size 지정
        line.positionCount = x.Count;        

        // 받아온 위치 데이터 배열의 길이만큼 반복
        for (int i = 0; i < line.positionCount; i++)
        { 
            // Positions 배열에 json으로 받아온 위치데이터 좌표값 추가
            line.SetPosition(i, new Vector3(x[i], y[i], z[i]));
            // 라인렌더러 컴포넌트 활성화
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
        // UI Canvas를 비활성화
        if (Input.GetMouseButtonDown(0))
        {
            infoCanvas.SetActive(false);
            backgroundImage.SetActive(false);
        }
    }
}
