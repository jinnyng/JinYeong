using System.Collections.Generic;

namespace LitJsonData.ListofData
{
    [System.Serializable]
    public class SatelliteData
    {
        public int No;
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
        public List<SatelliteData> Satellite;
    }
}

