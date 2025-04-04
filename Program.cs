using System;
using System.Collections.Generic;
using System.Threading;
using System.Configuration;
using NLog;
using S7.Net;
using System.Data;
using System.Data.SqlClient;


namespace PYVS_RollingLengthCollector
{
    class Program
    {
        //define members
        private static readonly int _delay = 1000;    //main delay time

        private static Plc plc;
        private static string ipaddr_UFRPLC = "172.18.12.1";    //PLC address   
        private static short rack = 0; //PLC Rack
        private static short slot = 6; //Slot --> CPU#2

        

        private static string _conStringRML = "Data Source=172.18.72.201\\LEVEL2;Initial Catalog=POSCOHSM_RML;Persist Security Info=True;User ID=POSCOHSM;Password=POSCOHSM;Application Name = RollingLengthCollector; Pooling=true;Max Pool Size=30;Connection Lifetime = 120; Connect Timeout = 20";
        private static string _conStringEXT = "Data Source=172.18.72.201\\LEVEL2;Initial Catalog=POSCOHSM_EXT;Persist Security Info=True;User ID=POSCOHSM;Password=POSCOHSM;Application Name = RollingLengthCollector; Pooling=true;Max Pool Size=30;Connection Lifetime = 120; Connect Timeout = 20";

        private static string evtaddr = "DB215.DBX117.2";

        //
        private static bool bConnectStatus = false;
        private static int lastMaterialID = 0;
        private struct LengthData
        {
            public string lotNo;
            public int seqNo;
            public int exitLength;
            public int cutLength1;
            public int cutLength2;
            public int sampleLength;
            public int tailLength;
            public int headLength;

            public void initData()
            {
                lotNo = null;
                seqNo = 0;
                exitLength = 0;
                cutLength1 = 0;
                cutLength2 = 0;
                sampleLength = 0;
                headLength = 0;
                tailLength = 0;
            }
        };
        private static LengthData lenData;

        //define logging
        private static Logger _log = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            //set console size(width,height)
            Console.SetWindowSize(80, 30);

            _log.Info("==================================================");
            _log.Info("PYVS_RollingLengthCollector Process start...");
            _log.Info("Location : {0}", Environment.CurrentDirectory);
            _log.Info("==================================================");

            try
            {
                //PLC connectivity information
                plc = new Plc(CpuType.S7300, ipaddr_UFRPLC, rack, slot);

                while (true)
                {
                    if (bConnectStatus == false)
                    {
                        InitConnections();  //Communication initialize
                    }
                    else //Communication init success
                    {
                        //Check Hot saw working finish status
                        bool evt = (bool)plc.Read(evtaddr);
                        if (evt == true)    //HowSaw#2 work finish
                        {
                            //Data read ID and compare with last collect ID
                            int trkID = ((uint)plc.Read("DB242.DBD0")).ConvertToInt();  //Read Dint

                            if (trkID > 0 && lastMaterialID != trkID) //exist material and send data only one time
                            {
                                Console.WriteLine(".");
                                _log.Info("Length Data Collecting Start !!! SP-ID : [{0}]", trkID);

                                //Data Collect
                                lenData.initData();
                                lenData.exitLength = ((ushort)plc.Read("DB242.DBW4")).ConvertToShort();
                                lenData.cutLength1 = ((ushort)plc.Read("DB242.DBW6")).ConvertToShort();
                                lenData.cutLength2 = ((ushort)plc.Read("DB242.DBW8")).ConvertToShort();
                                lenData.sampleLength = ((ushort)plc.Read("DB242.DBW10")).ConvertToShort();
                                lenData.tailLength = ((ushort)plc.Read("DB242.DBW12")).ConvertToShort();

                                //data collect from HMI DB
                                //lenData.exitLength = ((ushort)plc.Read("DB220.DBW2202")).ConvertToShort();
                                //lenData.cutLength1 = ((ushort)plc.Read("DB220.DBW2040")).ConvertToShort();
                                //lenData.cutLength2 = ((ushort)plc.Read("DB220.DBW2042")).ConvertToShort();
                                //lenData.sampleLength = ((ushort)plc.Read("DB220.DBW2024")).ConvertToShort();
                                //lenData.tailLength = ((ushort)plc.Read("DB220.DBW2046")).ConvertToShort();

                                //Check exit length and 1st cutting length (must have value)
                                if (lenData.exitLength > 0 || lenData.cutLength1 > 0)
                                {
                                    //Get LOT informaiton 
                                    if (GetLotInfo(trkID, ref lenData))
                                    {
                                        //
                                        _log.Info("Collected Length Data ----------------------------");
                                        _log.Info("PLCID [{0}] / LOT[{1}] / Seq[{2}]", trkID, lenData.lotNo, lenData.seqNo);
                                        _log.Info("UFR Exit Length Total : {0}", lenData.exitLength);
                                        _log.Info("1st Cutting Length : {0}", lenData.cutLength1);
                                        _log.Info("2nd Cutting Length : {0}", lenData.cutLength2);
                                        _log.Info("Sample Cutting Length : {0}", lenData.sampleLength);
                                        _log.Info("Tail Cutting Length : {0}", lenData.tailLength);
                                        _log.Info("Head Cutting Length : {0}", lenData.headLength);

                                        //Data send to MES
                                        if (SendMessagetoMES(lenData))
                                        {
                                            //Update last sent tracking ID
                                            lastMaterialID = trkID;
                                        }

                                    }
                                    else //if (GetLotInfo(result, ref lenData))
                                    {
                                        _log.Warn("SP-ID:[{0}] - LOT info still not have...", trkID);
                                    }
                                }
                                else //if (exitLength > 0 || cutLength1 > 0)
                                {
                                    //Length data error
                                    _log.Warn("Collect length data wrong !!! exitLength{0}, cutLength1{1}", lenData.exitLength, lenData.cutLength1);
                                }
                            }
                            else //if (trkID > 0 && lastMaterialID != trkID) 
                            {
                                if (trkID <= 0)
                                {
                                    _log.Warn("SP-ID : {0} is wrong !!!", trkID);
                                }
                                else
                                {
                                    Console.Write("=");
                                }
                            }
                        }
                        else //if (evt == true)
                        {
                            Console.Write(".");
                        }

                    }

                    //_log.Info("------------------------------------------------");
                    Thread.Sleep(_delay);
                }
            }
            catch (PlcException pex)
            {
                _log.Error(pex.Message);
                bConnectStatus = false; //re-init connections
            }
            catch (Exception ex)
            {
                _log.Error(ex.Message);
                bConnectStatus = false; //re-init connections
            }

        }


        /// <summary>
        /// Initialize database and PLC connection 
        /// </summary>
        private static void InitConnections()
        {
            bool bConnPlc = false;
            bool bDbRML = false;
            bool bDbEXT = false;

            _log.Warn("Connection initialize start.....");

            try
            {
                // Connect PLC
                if (plc.IsConnected == false)
                {
                    try
                    {
                        plc.Open();
                        bConnPlc = true;
                        _log.Info("PLC communication initialize success...");
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }

                // Connect database
                using (SqlConnection conn1 = new SqlConnection(_conStringRML))
                {
                    conn1.Open();
                    bDbRML = true;
                }
                _log.Info("Database RML initialize success...");

                // Connect database
                using (SqlConnection conn2 = new SqlConnection(_conStringEXT))
                {
                    conn2.Open();
                    bDbEXT = true;
                }
                _log.Info("Database EXT initialize success...");
            }
            catch (Exception e)
            {
                _log.Error(e.Message);
            }

            if (bConnPlc && bDbRML && bDbEXT)
            {
                bConnectStatus = true;
            }
        }


        /// <summary>
        /// Collect semiproduct Lot number and sequence number
        /// </summary>
        private static bool GetLotInfo(int plcId, ref LengthData rdata)
        {
            bool bcollect = false;

            try
            {
                // Get current casting strand information
                string sql = "SELECT RML_JOB.JOB_CODE, RML_SEMIPRODUCT.SEMIPRODUCT_NO FROM RML_SEMIPRODUCT, RML_PROGRAM, RML_JOB " +
                             "WHERE SEMIPRODUCT_PLC_CODE = " + plcId + 
                             " AND RML_SEMIPRODUCT.PROGRAM_ID = RML_PROGRAM.PROGRAM_ID AND RML_PROGRAM.JOB_ID = RML_JOB.JOB_ID";
                //_log.Info(sql);

                using (SqlConnection con = new SqlConnection(_conStringRML))
                using (SqlCommand cmd = new SqlCommand(sql, con))
                {
                    cmd.CommandType = CommandType.Text;
                    con.Open();

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                rdata.lotNo = reader["JOB_CODE"].ToString().Trim();
                                rdata.seqNo = Convert.ToInt16(reader["SEMIPRODUCT_NO"]);
                                bcollect = true;
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
                _log.Warn("GetLotInfo() Error... Data read error!!!");
                //bConnectStatus = false; //disconnect??? - falling to unlimit loop?
                return bcollect;
            }
            return bcollect;
        }


        /// <summary>
        /// Make MES Message
        /// </summary>
        private static bool SendMessagetoMES(LengthData sdata)
        {
            string msgHead = string.Empty;
            string msgData = string.Empty;
            string sql = string.Empty;

            try
            {
                //Set message - Header
                msgHead += "M30RL12E";                          //TC Code
                msgHead += "2000";                              //send factory code
                msgHead += "L12";                               //sending process code
                msgHead += "2000";                              //receive factory code
                msgHead += "M30";                               //receive process code
                msgHead += DateTime.Now.ToString("yyyyMMddHHmmss");     //sending date time
                msgHead += "L3SentrySectio";                    //send program ID
                msgHead += "RM30L12_07".PadRight(19);           //EAI IF ID (Queue name)
                msgHead += "000150".PadRight(31);               //message length & spare

                //Set message - Data
                msgData += "2";                                 //FactoryType 'Section (2)
                msgData += sdata.lotNo;                         //Rolling Lot Number 
                msgData += sdata.seqNo.ToString("D5");          //Rolling Lot Sequence number
                msgData += sdata.exitLength.ToString("D7");     //Bar Length after UFR Exit
                msgData += sdata.cutLength1.ToString("D6");     //1st cutting Length
                msgData += sdata.cutLength2.ToString("D6");     //2nd Cutting Length
                msgData += sdata.sampleLength.ToString("D5");   //Sample Cutting Length
                msgData += sdata.headLength.ToString("D5");     //Head Cutting Length
                msgData += sdata.tailLength.ToString("D5");     //Tail Cutting Length

                //Query
                sql = "INSERT INTO TT_L2_L3_NEW (MSG_COUNTER, DATETIME, MSG_CODE, HEADER, DATA, INTERFACE_ID) VALUES (" +
                      "(SELECT MAX(MSG_COUNTER)+1 FROM TT_L2_L3_NEW), " +
                      "GETDATE(), " +
                      "'M30RL12E'," +
                      "'" + msgHead + "'," +
                      "'" + msgData + "'," +                      
                      "'RM30L12_07')";

                _log.Debug("SQL : {0}", sql);

                using (SqlConnection con = new SqlConnection(_conStringEXT))
                {
                    SqlCommand cmd = new SqlCommand(sql, con);
                    cmd.CommandType = CommandType.Text;
                    con.Open();

                    int rowsAffected = cmd.ExecuteNonQuery();
                }
                _log.Info(" --- Data Send Compelte !!!");

            }
            catch (Exception ex)
            {
                _log.Warn("MES data Sending error! Msg:{0}", ex.Message);
                return false;
            }
            return true;
        }
    }
}
