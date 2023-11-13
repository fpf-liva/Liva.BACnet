/**************************************************************************
*                           MIT License
* 
* Copyright (C) 2014 Morten Kvistgaard <mk@pch-engineering.dk>
*
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the
* "Software"), to deal in the Software without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
*
* The above copyright notice and this permission notice shall be included
* in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
* CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
* TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
* SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*
*********************************************************************/

using System.Diagnostics;
using System.IO.BACnet;
using System.IO.BACnet.Storage;
using BacnetWriteAccessSpecification = System.IO.BACnet.BacnetWriteAccessSpecification;

namespace Liva.BACnet.Device;

internal abstract class Program
{
    private static DeviceStorage? _storage;
    private static BacnetClient? _ipServer;
    private static BacnetClient? _mstpServer;
    private static BacnetClient? _ptpServer;
    private static readonly Dictionary<BacnetObjectId, List<Subscription>> Subscriptions = new();
    private static readonly object LockObject = new();
    private static readonly BacnetSegmentations SupportedSegmentation = BacnetSegmentations.SEGMENTATION_BOTH;

    private static void Main()
    {
        try
        {
            //init
            Trace.Listeners.Add(new ConsoleTraceListener());    //Some of the classes are using the Trace/Debug to communicate loggin. (As they should.) So let's catch those as well.
            _storage = DeviceStorage.Load("LIVA-EMS.xml");
            _storage.ChangeOfValue += new DeviceStorage.ChangeOfValueHandler(m_storage_ChangeOfValue);
            _storage.ReadOverride += new DeviceStorage.ReadOverrideHandler(m_storage_ReadOverride);

            //create udp service point
            BacnetIpUdpProtocolTransport udp_transport = new BacnetIpUdpProtocolTransport(0xBAC0, false);       //set to true to force "single socket" usage
            _ipServer = new BacnetClient(udp_transport);
            _ipServer.OnWhoIs += new BacnetClient.WhoIsHandler(OnWhoIs);
            _ipServer.OnWhoHas += new BacnetClient.WhoHasHandler(OnWhoHas);
            _ipServer.OnReadPropertyRequest += new BacnetClient.ReadPropertyRequestHandler(OnReadPropertyRequest);
            _ipServer.OnWritePropertyRequest += new BacnetClient.WritePropertyRequestHandler(OnWritePropertyRequest);
            _ipServer.OnReadPropertyMultipleRequest += new BacnetClient.ReadPropertyMultipleRequestHandler(OnReadPropertyMultipleRequest);
            _ipServer.OnWritePropertyMultipleRequest += new BacnetClient.WritePropertyMultipleRequestHandler(OnWritePropertyMultipleRequest);
            _ipServer.OnAtomicWriteFileRequest += new BacnetClient.AtomicWriteFileRequestHandler(OnAtomicWriteFileRequest);
            _ipServer.OnAtomicReadFileRequest += new BacnetClient.AtomicReadFileRequestHandler(OnAtomicReadFileRequest);
            _ipServer.OnSubscribeCOV += new BacnetClient.SubscribeCOVRequestHandler(OnSubscribeCOV);
            _ipServer.OnSubscribeCOVProperty += new BacnetClient.SubscribeCOVPropertyRequestHandler(OnSubscribeCOVProperty);
            _ipServer.OnTimeSynchronize += new BacnetClient.TimeSynchronizeHandler(OnTimeSynchronize);
            _ipServer.OnDeviceCommunicationControl += new BacnetClient.DeviceCommunicationControlRequestHandler(OnDeviceCommunicationControl);
            _ipServer.OnReinitializedDevice += new BacnetClient.ReinitializedRequestHandler(OnReinitializedDevice);
            _ipServer.OnIam += new BacnetClient.IamHandler(OnIam);
            _ipServer.OnReadRange += new BacnetClient.ReadRangeHandler(OnReadRange);
            _ipServer.Start();

            //create pipe (MSTP) service point
            BacnetPipeTransport pipe_transport = new BacnetPipeTransport("COM1003", true);
            BacnetMstpProtocolTransport mstp_transport = new BacnetMstpProtocolTransport(pipe_transport, 0, 127, 1);
            mstp_transport.StateLogging = false;        //if you enable this, it will display a lot of information about the StateMachine
            _mstpServer = new BacnetClient(mstp_transport);
            _mstpServer.OnWhoIs += new BacnetClient.WhoIsHandler(OnWhoIs);
            _mstpServer.OnWhoHas += new BacnetClient.WhoHasHandler(OnWhoHas);
            _mstpServer.OnReadPropertyRequest += new BacnetClient.ReadPropertyRequestHandler(OnReadPropertyRequest);
            _mstpServer.OnWritePropertyRequest += new BacnetClient.WritePropertyRequestHandler(OnWritePropertyRequest);
            _mstpServer.OnReadPropertyMultipleRequest += new BacnetClient.ReadPropertyMultipleRequestHandler(OnReadPropertyMultipleRequest);
            _mstpServer.OnWritePropertyMultipleRequest += new BacnetClient.WritePropertyMultipleRequestHandler(OnWritePropertyMultipleRequest);
            _mstpServer.OnAtomicWriteFileRequest += new BacnetClient.AtomicWriteFileRequestHandler(OnAtomicWriteFileRequest);
            _mstpServer.OnAtomicReadFileRequest += new BacnetClient.AtomicReadFileRequestHandler(OnAtomicReadFileRequest);
            _mstpServer.OnSubscribeCOV += new BacnetClient.SubscribeCOVRequestHandler(OnSubscribeCOV);
            _mstpServer.OnSubscribeCOVProperty += new BacnetClient.SubscribeCOVPropertyRequestHandler(OnSubscribeCOVProperty);
            _mstpServer.OnTimeSynchronize += new BacnetClient.TimeSynchronizeHandler(OnTimeSynchronize);
            _mstpServer.OnDeviceCommunicationControl += new BacnetClient.DeviceCommunicationControlRequestHandler(OnDeviceCommunicationControl);
            _mstpServer.OnReinitializedDevice += new BacnetClient.ReinitializedRequestHandler(OnReinitializedDevice);
            _mstpServer.OnIam += new BacnetClient.IamHandler(OnIam);
            _mstpServer.OnReadRange += new BacnetClient.ReadRangeHandler(OnReadRange);
            _mstpServer.Start();

            //create pipe (PTP) service point
            BacnetPipeTransport pipe2_transport = new BacnetPipeTransport("COM1004", true);
            BacnetPtpProtocolTransport ptp_transport = new BacnetPtpProtocolTransport(pipe2_transport, true);
            ptp_transport.StateLogging = false;        //if you enable this, it will display a lot of information
            _ptpServer = new BacnetClient(ptp_transport);
            _ptpServer.OnWhoIs += new BacnetClient.WhoIsHandler(OnWhoIs);
            _ptpServer.OnWhoHas += new BacnetClient.WhoHasHandler(OnWhoHas);
            _ptpServer.OnReadPropertyRequest += new BacnetClient.ReadPropertyRequestHandler(OnReadPropertyRequest);
            _ptpServer.OnWritePropertyRequest += new BacnetClient.WritePropertyRequestHandler(OnWritePropertyRequest);
            _ptpServer.OnReadPropertyMultipleRequest += new BacnetClient.ReadPropertyMultipleRequestHandler(OnReadPropertyMultipleRequest);
            _ptpServer.OnWritePropertyMultipleRequest += new BacnetClient.WritePropertyMultipleRequestHandler(OnWritePropertyMultipleRequest);
            _ptpServer.OnAtomicWriteFileRequest += new BacnetClient.AtomicWriteFileRequestHandler(OnAtomicWriteFileRequest);
            _ptpServer.OnAtomicReadFileRequest += new BacnetClient.AtomicReadFileRequestHandler(OnAtomicReadFileRequest);
            _ptpServer.OnSubscribeCOV += new BacnetClient.SubscribeCOVRequestHandler(OnSubscribeCOV);
            _ptpServer.OnSubscribeCOVProperty += new BacnetClient.SubscribeCOVPropertyRequestHandler(OnSubscribeCOVProperty);
            _ptpServer.OnTimeSynchronize += new BacnetClient.TimeSynchronizeHandler(OnTimeSynchronize);
            _ptpServer.OnDeviceCommunicationControl += new BacnetClient.DeviceCommunicationControlRequestHandler(OnDeviceCommunicationControl);
            _ptpServer.OnReinitializedDevice += new BacnetClient.ReinitializedRequestHandler(OnReinitializedDevice);
            _ptpServer.OnIam += new BacnetClient.IamHandler(OnIam);
            _ptpServer.OnReadRange += new BacnetClient.ReadRangeHandler(OnReadRange);
            _ptpServer.Start();

            //display info
            Console.WriteLine("DemoServer startet ...");
            Console.WriteLine("Udp service point - port: 0x" + udp_transport.SharedPort.ToString("X4") + "" + (udp_transport.ExclusivePort != udp_transport.SharedPort ? " and 0x" + udp_transport.ExclusivePort.ToString("X4") : ""));
            Console.WriteLine("MSTP service point - name: \\\\.pipe\\" + pipe_transport.Name + ", source_address: " + mstp_transport.SourceAddress + ", max_master: " + mstp_transport.MaxMaster + ", max_info_frames: " + mstp_transport.MaxInfoFrames);
            Console.WriteLine("PTP service point - name: \\\\.pipe\\" + pipe2_transport.Name);
            Console.WriteLine("");

            //send greeting
            _ipServer.Iam(_storage.DeviceId, SupportedSegmentation);
            _mstpServer.Iam(_storage.DeviceId, SupportedSegmentation);
                
            //endless loop of nothing
            Console.WriteLine("Press the ANY key to exit!");
            while (!Console.KeyAvailable)
            {
                //Endless loops of nothing are rather pointless, but I was too lazy to do anything fancy. 
                //And to be honest, it's not like it's sucking up a lot of system resources. 
                //If we'd made a GUI program or a Win32 service, we wouldn't have needed this. 
                System.Threading.Thread.Sleep(1000);            
            }
            Console.ReadKey();
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception: " + ex.Message);
        }
        finally
        {
            Console.WriteLine("Press the ANY key ... once more");
            Console.ReadKey();
        }
    }

    private static BacnetLogRecord[] m_trend_samples = null;

    private static byte[] GetEncodedTrends(uint start, int count, out BacnetResultFlags status)
    {
        status = BacnetResultFlags.NONE;
        start--;    //position is 1 based

        if (start >= m_trend_samples.Length || (start + count) > m_trend_samples.Length)
        {
            Trace.TraceError("Trend data read overflow");
            return null;
        }

        if (start == 0) status |= BacnetResultFlags.FIRST_ITEM;
        if ((start + count) >= m_trend_samples.Length) status |= BacnetResultFlags.LAST_ITEM;
        else status |= BacnetResultFlags.MORE_ITEMS;

        System.IO.BACnet.Serialize.EncodeBuffer buffer = new System.IO.BACnet.Serialize.EncodeBuffer();
        for (uint i = start; i < (start + count); i++)
        {
            System.IO.BACnet.Serialize.Services.EncodeLogRecord(buffer, m_trend_samples[i]);
        }

        return buffer.ToArray();
    }

    private static void OnReadRange(BacnetClient sender, BacnetAddress adr, byte invoke_id, BacnetObjectId objectId, BacnetPropertyReference property, System.IO.BACnet.Serialize.BacnetReadRangeRequestTypes requestType, uint position, DateTime time, int count, BacnetMaxSegments max_segments)
    {
        if (objectId.type == BacnetObjectTypes.OBJECT_TRENDLOG && objectId.instance == 0)
        {
            //generate 100 samples
            if (m_trend_samples == null)
            {
                m_trend_samples = new BacnetLogRecord[100];
                DateTime current = DateTime.Now.AddSeconds(-m_trend_samples.Length);
                Random rnd = new Random();
                for (int i = 0; i < m_trend_samples.Length; i++)
                {
                    m_trend_samples[i] = new BacnetLogRecord(BacnetTrendLogValueType.TL_TYPE_UNSIGN, (uint)rnd.Next(0, 100), current, 0);
                    current = current.AddSeconds(1);
                }
            }

            //encode
            BacnetResultFlags status;
            byte[] application_data = GetEncodedTrends(position, count, out status);

            //send
            sender.ReadRangeResponse(adr, invoke_id, sender.GetSegmentBuffer(max_segments), objectId, property, status, (uint)count, application_data, requestType, position);
                
        }
        else
            sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_RANGE, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);    
    }

    private static void OnIam(BacnetClient sender, BacnetAddress adr, uint device_id, uint max_apdu, BacnetSegmentations segmentation, ushort vendor_id)
    {
        //ignore Iams from other devices. (Also loopbacks)
    }

    /*****************************************************************************************************/
    // OnWhoHas by thamersalek
    static void OnWhoHas(BacnetClient sender, BacnetAddress adr, int low_limit, int high_limit, BacnetObjectId ObjId, string ObjName)
    {
        if ((low_limit == -1 && high_limit == -1) || (_storage.DeviceId >= low_limit && _storage.DeviceId <= high_limit))
        {
            BacnetObjectId deviceid1 = new BacnetObjectId(BacnetObjectTypes.OBJECT_DEVICE, _storage.DeviceId);

            lock (_storage)
            {
                if (ObjName != null)
                {
                    foreach (System.IO.BACnet.Storage.Object Obj in _storage.Objects)
                    {
                        foreach (Property p in Obj.Properties)
                        {
                            if (p.Id==BacnetPropertyIds.PROP_OBJECT_NAME) // only Object Name property
                                if (p.Tag == BacnetApplicationTags.BACNET_APPLICATION_TAG_CHARACTER_STRING) // it should be
                                {
                                    if (p.Value[0] == ObjName)
                                    {
                                        BacnetObjectId objid2 = new BacnetObjectId((BacnetObjectTypes)Obj.Type, Obj.Instance);
                                        sender.IHave(deviceid1, objid2, ObjName);
                                        return; // done
                                    }
                                }
                        }
                    }

                }
                else
                {
                    System.IO.BACnet.Storage.Object obj = _storage.FindObject(ObjId);
                    if (obj != null)
                    {
                        foreach (Property p in obj.Properties) // object name is mandatory
                        {
                            if ((p.Id == BacnetPropertyIds.PROP_OBJECT_NAME)&&(p.Tag == BacnetApplicationTags.BACNET_APPLICATION_TAG_CHARACTER_STRING))
                            {
                                sender.IHave(deviceid1, ObjId, p.Value[0]);
                                return; // done
                            }
                        }
                    }

                }

            }
        }
    }
    /// <summary>
    /// This function is for overriding some of the default responses. Meaning that it's for ugly hacks!
    /// You'll need this kind of 'dynamic' function when working with storages that're static by nature.
    /// </summary>
    private static void m_storage_ReadOverride(BacnetObjectId object_id, BacnetPropertyIds property_id, uint array_index, out IList<BacnetValue> value, out DeviceStorage.ErrorCodes status, out bool handled)
    {
        handled = true;
        value = new BacnetValue[0];
        status = DeviceStorage.ErrorCodes.Good;

        if (object_id.type == BacnetObjectTypes.OBJECT_DEVICE && property_id == BacnetPropertyIds.PROP_OBJECT_LIST)
        {
            if (array_index == 0)
            {
                //object list count 
                value = new BacnetValue[] { new BacnetValue(BacnetApplicationTags.BACNET_APPLICATION_TAG_UNSIGNED_INT, (uint)_storage.Objects.Length) };
            }
            else if (array_index != System.IO.BACnet.Serialize.ASN1.BACNET_ARRAY_ALL)
            {
                //object list index 
                value = new BacnetValue[] { new BacnetValue(BacnetApplicationTags.BACNET_APPLICATION_TAG_OBJECT_ID, new BacnetObjectId(_storage.Objects[array_index - 1].Type, _storage.Objects[array_index-1].Instance)) };
            }
            else
            {
                //object list whole
                BacnetValue[] list = new BacnetValue[_storage.Objects.Length];
                for (int i = 0; i < list.Length; i++)
                {
                    list[i].Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_OBJECT_ID;
                    list[i].Value = new BacnetObjectId(_storage.Objects[i].Type, _storage.Objects[i].Instance);
                }
                value = list;
            }
        }
        else if (object_id.type == BacnetObjectTypes.OBJECT_DEVICE && object_id.instance == _storage.DeviceId && property_id == BacnetPropertyIds.PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED)
        {
            BacnetValue v = new BacnetValue();
            v.Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_BIT_STRING;
            BacnetBitString b = new BacnetBitString();
            b.SetBit((byte)BacnetObjectTypes.MAX_ASHRAE_OBJECT_TYPE, false); //set all false
            b.SetBit((byte)BacnetObjectTypes.OBJECT_ANALOG_INPUT, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_ANALOG_OUTPUT, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_ANALOG_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_BINARY_INPUT, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_BINARY_OUTPUT, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_BINARY_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_DEVICE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_FILE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_MULTI_STATE_INPUT, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_MULTI_STATE_OUTPUT, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_MULTI_STATE_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_BITSTRING_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_CHARACTERSTRING_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_OCTETSTRING_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_DATE_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_DATETIME_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_TIME_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_INTEGER_VALUE, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_GROUP, true);
            b.SetBit((byte)BacnetObjectTypes.OBJECT_STRUCTURED_VIEW, true);
            //there're prolly more, who knows
            v.Value = b;
            value = new BacnetValue[] { v };
        }
        else if (object_id.type == BacnetObjectTypes.OBJECT_DEVICE && object_id.instance == _storage.DeviceId && property_id == BacnetPropertyIds.PROP_PROTOCOL_SERVICES_SUPPORTED)
        {
            BacnetValue v = new BacnetValue();
            v.Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_BIT_STRING;
            BacnetBitString b = new BacnetBitString();
            b.SetBit((byte)BacnetServicesSupported.MAX_BACNET_SERVICES_SUPPORTED, false); //set all false
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_I_AM, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_WHO_IS, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_WHO_HAS, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_READ_PROP_MULTIPLE, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_READ_PROPERTY, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_WRITE_PROPERTY, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_WRITE_PROP_MULTIPLE, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_ATOMIC_READ_FILE, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_ATOMIC_WRITE_FILE, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_CONFIRMED_COV_NOTIFICATION, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_UNCONFIRMED_COV_NOTIFICATION, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_SUBSCRIBE_COV, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_SUBSCRIBE_COV_PROPERTY, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_REINITIALIZE_DEVICE, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_DEVICE_COMMUNICATION_CONTROL, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_TIME_SYNCHRONIZATION, true);
            b.SetBit((byte)BacnetServicesSupported.SERVICE_SUPPORTED_UTC_TIME_SYNCHRONIZATION, true);
            v.Value = b;
            value = new BacnetValue[] { v };
        }
        else if (object_id.type == BacnetObjectTypes.OBJECT_DEVICE && object_id.instance == _storage.DeviceId && property_id == BacnetPropertyIds.PROP_SEGMENTATION_SUPPORTED)
        {
            BacnetValue v = new BacnetValue();
            v.Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_ENUMERATED;
            v.Value = (uint)BacnetSegmentations.SEGMENTATION_BOTH;
            value = new BacnetValue[] { v };
        }
        else if (object_id.type == BacnetObjectTypes.OBJECT_DEVICE && object_id.instance == _storage.DeviceId && property_id == BacnetPropertyIds.PROP_SYSTEM_STATUS)
        {
            BacnetValue v = new BacnetValue();
            v.Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_ENUMERATED;
            v.Value = (uint)BacnetDeviceStatus.OPERATIONAL;      //can we be in any other mode I wonder?
            value = new BacnetValue[] { v };
        }
        else if (object_id.type == BacnetObjectTypes.OBJECT_DEVICE && object_id.instance == _storage.DeviceId && property_id == BacnetPropertyIds.PROP_ACTIVE_COV_SUBSCRIPTIONS)
        {
            List<BacnetValue> list = new List<BacnetValue>();
            foreach (KeyValuePair<BacnetObjectId, List<Subscription>> entry in Subscriptions)
            {
                foreach (Subscription sub in entry.Value)
                {
                    //encode
                    //System.IO.BACnet.Serialize.EncodeBuffer buffer = new System.IO.BACnet.Serialize.EncodeBuffer();
                    BacnetCOVSubscription cov = new BacnetCOVSubscription();
                    cov.Recipient = sub.reciever_address;
                    cov.subscriptionProcessIdentifier = sub.subscriberProcessIdentifier;
                    cov.monitoredObjectIdentifier = sub.monitoredObjectIdentifier;
                    cov.monitoredProperty = sub.monitoredProperty;
                    cov.IssueConfirmedNotifications = sub.issueConfirmedNotifications;
                    cov.TimeRemaining = (uint)sub.lifetime - (uint)(DateTime.Now - sub.start).TotalMinutes;
                    cov.COVIncrement = sub.covIncrement;
                    //System.IO.BACnet.Serialize.ASN1.encode_cov_subscription(buffer, cov);

                    //add
                    //BacnetValue v = new BacnetValue();
                    //v.Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_COV_SUBSCRIPTION;
                    //v.Value = buffer.ToArray();
                    list.Add(new BacnetValue(cov));
                }
            }
            value = list;
        }
        else if (object_id.type == BacnetObjectTypes.OBJECT_OCTETSTRING_VALUE && object_id.instance == 0 && property_id == BacnetPropertyIds.PROP_PRESENT_VALUE)
        {
            //this is our huge blob
            BacnetValue v = new BacnetValue();
            v.Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_OCTET_STRING;
            byte[] blob = new byte[2000];
            for(int i = 0; i < blob.Length; i++)
                blob[i] = (i % 2 == 0) ? (byte)'A' : (byte)'B';
            v.Value = blob;
            value = new BacnetValue[] { v };
        }
        else if (object_id.type == BacnetObjectTypes.OBJECT_GROUP && property_id == BacnetPropertyIds.PROP_PRESENT_VALUE)
        {
            //get property list
            IList<BacnetValue> properties;
            if (_storage.ReadProperty(object_id, BacnetPropertyIds.PROP_LIST_OF_GROUP_MEMBERS, System.IO.BACnet.Serialize.ASN1.BACNET_ARRAY_ALL, out properties) != DeviceStorage.ErrorCodes.Good)
            {
                value = new BacnetValue[] { new BacnetValue(BacnetApplicationTags.BACNET_APPLICATION_TAG_ERROR, new BacnetError(BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_INTERNAL_ERROR)) };
            }
            else
            {
                List<BacnetValue> _value = new List<BacnetValue>();
                foreach (BacnetValue p in properties)
                {
                    if (p.Value is BacnetReadAccessSpecification)
                    {
                        BacnetReadAccessSpecification prop = (BacnetReadAccessSpecification)p.Value;
                        BacnetReadAccessResult result = new BacnetReadAccessResult();
                        result.objectIdentifier = prop.objectIdentifier;
                        List<BacnetPropertyValue> result_values = new List<BacnetPropertyValue>();
                        foreach (BacnetPropertyReference r in prop.propertyReferences)
                        {
                            BacnetPropertyValue prop_value = new BacnetPropertyValue();
                            prop_value.property = r;
                            if (_storage.ReadProperty(prop.objectIdentifier, (BacnetPropertyIds)r.propertyIdentifier, r.propertyArrayIndex, out prop_value.value) != DeviceStorage.ErrorCodes.Good)
                            {
                                prop_value.value = new BacnetValue[] { new BacnetValue(BacnetApplicationTags.BACNET_APPLICATION_TAG_ERROR, new BacnetError(BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_INTERNAL_ERROR)) };
                            }
                            result_values.Add(prop_value);
                        }
                        result.values = result_values;
                        _value.Add(new BacnetValue(BacnetApplicationTags.BACNET_APPLICATION_TAG_READ_ACCESS_RESULT, result));
                    }
                }
                value = _value;
            }
        }
        else
        {
            handled = false;
        }
    }

    private class Subscription
    {
        public BacnetClient reciever;
        public BacnetAddress reciever_address;
        public uint subscriberProcessIdentifier;
        public BacnetObjectId monitoredObjectIdentifier;
        public BacnetPropertyReference monitoredProperty;
        public bool issueConfirmedNotifications;
        public uint lifetime;
        public DateTime start;
        public float covIncrement;
        public Subscription(BacnetClient reciever, BacnetAddress reciever_address, uint subscriberProcessIdentifier, BacnetObjectId monitoredObjectIdentifier, BacnetPropertyReference property, bool issueConfirmedNotifications, uint lifetime, float covIncrement)
        {
            this.reciever = reciever;
            this.reciever_address = reciever_address;
            this.subscriberProcessIdentifier = subscriberProcessIdentifier;
            this.monitoredObjectIdentifier = monitoredObjectIdentifier;
            this.monitoredProperty = property;
            this.issueConfirmedNotifications = issueConfirmedNotifications;
            this.lifetime = lifetime;
            this.start = DateTime.Now;
            this.covIncrement = covIncrement;
        }

        /// <summary>
        /// Returns the remaining subscription time. Negative values will imply that the subscription has to be removed.
        /// Value 0 *may* imply that the subscription doesn't have a timeout
        /// </summary>
        /// <returns></returns>
        public int GetTimeRemaining()
        {
            if (lifetime == 0) return 0;
            else return (int)lifetime - (int)(DateTime.Now - start).TotalSeconds;
        }
    }

    private static void RemoveOldSubscriptions()
    {
        LinkedList<BacnetObjectId> to_be_deleted = new LinkedList<BacnetObjectId>();
        foreach (KeyValuePair<BacnetObjectId, List<Subscription>> entry in Subscriptions)
        {
            for (int i = 0; i < entry.Value.Count; i++)
            {
                if (entry.Value[i].GetTimeRemaining() < 0)
                {
                    Trace.TraceWarning("Removing old subscription: " + entry.Key.ToString());
                    entry.Value.RemoveAt(i);
                    i--;
                }
            }
            if (entry.Value.Count == 0)
                to_be_deleted.AddLast(entry.Key);
        }
        foreach (BacnetObjectId obj_id in to_be_deleted)
        {
            Subscriptions.Remove(obj_id);
        }
    }

    private static Subscription HandleSubscriptionRequest(BacnetClient sender, BacnetAddress adr, byte invoke_id, uint subscriberProcessIdentifier, BacnetObjectId monitoredObjectIdentifier, uint property_id, bool cancellationRequest, bool issueConfirmedNotifications, uint lifetime, float covIncrement)
    {
        //remove old leftovers
        RemoveOldSubscriptions();

        //find existing
        List<Subscription> subs = null;
        Subscription sub = null;
        if (Subscriptions.ContainsKey(monitoredObjectIdentifier))
        {
            subs = Subscriptions[monitoredObjectIdentifier];
            foreach (Subscription s in subs)
            {
                if (s.reciever == sender && s.reciever_address.Equals(adr) && s.subscriberProcessIdentifier == subscriberProcessIdentifier && s.monitoredObjectIdentifier.Equals(monitoredObjectIdentifier) && s.monitoredProperty.propertyIdentifier == property_id)
                {
                    sub = s;
                    break;
                }
            }
        }

        //cancel
        if (cancellationRequest && sub != null)
        {
            subs.Remove(sub);
            if (subs.Count == 0)
                Subscriptions.Remove(sub.monitoredObjectIdentifier);

            //send confirm
            sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV, invoke_id);

            return null;
        }

        //create if needed
        if (sub == null)
        {
            sub = new Subscription(sender, adr, subscriberProcessIdentifier, monitoredObjectIdentifier, new BacnetPropertyReference((uint)BacnetPropertyIds.PROP_ALL, System.IO.BACnet.Serialize.ASN1.BACNET_ARRAY_ALL), issueConfirmedNotifications, lifetime, covIncrement);
            if (subs == null)
            {
                subs = new List<Subscription>();
                Subscriptions.Add(sub.monitoredObjectIdentifier, subs);
            }
            subs.Add(sub);
        }

        //update perhaps
        sub.issueConfirmedNotifications = issueConfirmedNotifications;
        sub.lifetime = lifetime;
        sub.start = DateTime.Now;

        return sub;
    }

    private static void OnSubscribeCOV(BacnetClient sender, BacnetAddress adr, byte invoke_id, uint subscriberProcessIdentifier, BacnetObjectId monitoredObjectIdentifier, bool cancellationRequest, bool issueConfirmedNotifications, uint lifetime, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                //create (will also remove old subscriptions)
                Subscription sub = HandleSubscriptionRequest(sender, adr, invoke_id, subscriberProcessIdentifier, monitoredObjectIdentifier, (uint)BacnetPropertyIds.PROP_ALL, cancellationRequest, issueConfirmedNotifications, lifetime, 0);

                //send confirm
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV, invoke_id);

                //also send first values
                if (!cancellationRequest)
                {
                    System.Threading.ThreadPool.QueueUserWorkItem((o) =>
                    {
                        IList<BacnetPropertyValue> values;
                        if(_storage.ReadPropertyAll(sub.monitoredObjectIdentifier, out values))
                            if (!sender.Notify(adr, sub.subscriberProcessIdentifier, _storage.DeviceId, sub.monitoredObjectIdentifier, (uint)sub.GetTimeRemaining(), sub.issueConfirmedNotifications, values))
                                Trace.TraceError("Couldn't send notify");
                    }, null);
                }
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnSubscribeCOVProperty(BacnetClient sender, BacnetAddress adr, byte invoke_id, uint subscriberProcessIdentifier, BacnetObjectId monitoredObjectIdentifier, BacnetPropertyReference monitoredProperty, bool cancellationRequest, bool issueConfirmedNotifications, uint lifetime, float covIncrement, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                //create (will also remove old subscriptions)
                Subscription sub = HandleSubscriptionRequest(sender, adr, invoke_id, subscriberProcessIdentifier, monitoredObjectIdentifier, (uint)BacnetPropertyIds.PROP_ALL, cancellationRequest, issueConfirmedNotifications, lifetime, covIncrement);

                //send confirm
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV_PROPERTY, invoke_id);

                //also send first values
                if (!cancellationRequest)
                {
                    System.Threading.ThreadPool.QueueUserWorkItem((o) =>
                    {
                        IList<BacnetValue> _values;
                        _storage.ReadProperty(sub.monitoredObjectIdentifier, (BacnetPropertyIds)sub.monitoredProperty.propertyIdentifier, sub.monitoredProperty.propertyArrayIndex, out _values);
                        List<BacnetPropertyValue> values = new List<BacnetPropertyValue>();
                        BacnetPropertyValue tmp = new BacnetPropertyValue();
                        tmp.property = sub.monitoredProperty;
                        tmp.value = _values;
                        values.Add(tmp);
                        if (!sender.Notify(adr, sub.subscriberProcessIdentifier, _storage.DeviceId, sub.monitoredObjectIdentifier, (uint)sub.GetTimeRemaining(), sub.issueConfirmedNotifications, values))
                            Trace.TraceError("Couldn't send notify");
                    }, null);
                }
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV_PROPERTY, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void m_storage_ChangeOfValue(DeviceStorage sender, BacnetObjectId object_id, BacnetPropertyIds property_id, uint array_index, IList<BacnetValue> value)
    {
        System.Threading.ThreadPool.QueueUserWorkItem((o) =>
        {
            lock (LockObject)
            {
                //remove old leftovers
                RemoveOldSubscriptions();

                //find subscription
                if (!Subscriptions.ContainsKey(object_id)) return;
                List<Subscription> subs = Subscriptions[object_id];

                //convert
                List<BacnetPropertyValue> values = new List<BacnetPropertyValue>();
                BacnetPropertyValue tmp = new BacnetPropertyValue();
                tmp.property = new BacnetPropertyReference((uint)property_id, array_index);
                tmp.value = value;
                values.Add(tmp);

                //send to all
                foreach (Subscription sub in subs)
                {
                    if (sub.monitoredProperty.propertyIdentifier == (uint)BacnetPropertyIds.PROP_ALL || sub.monitoredProperty.propertyIdentifier == (uint)property_id)
                    {
                        //send notify
                        if (!sub.reciever.Notify(sub.reciever_address, sub.subscriberProcessIdentifier, _storage.DeviceId, sub.monitoredObjectIdentifier, (uint)sub.GetTimeRemaining(), sub.issueConfirmedNotifications, values))
                            Trace.TraceError("Couldn't send notify");
                    }
                }
            }
        }, null);
    }

    private static void OnDeviceCommunicationControl(BacnetClient sender, BacnetAddress adr, byte invoke_id, uint time_duration, uint enable_disable, string password, BacnetMaxSegments max_segments)
    {
        switch (enable_disable)
        {
            case 0:
                Trace.TraceInformation("Enable communication? Sure!");
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL, invoke_id);
                break;
            case 1:
                Trace.TraceInformation("Disable communication? ... smile and wave (ignored)");
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL, invoke_id);
                break;
            case 2:
                Trace.TraceWarning("Disable initiation? I don't think so!");
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
                break;
            default:
                Trace.TraceError("Now, what is this device_communication code: " + enable_disable + "!!!!");
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
                break;
        }
    }

    private static void OnReinitializedDevice(BacnetClient sender, BacnetAddress adr, byte invoke_id, BacnetReinitializedStates state, string password, BacnetMaxSegments max_segments)
    {
        Trace.TraceInformation("So you wanna reboot me, eh? Pfff! (" + state.ToString() + ")");
        sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_REINITIALIZE_DEVICE, invoke_id);
    }

    private static void OnTimeSynchronize(BacnetClient sender, BacnetAddress adr, DateTime dateTime, bool utc)
    {
        Trace.TraceInformation("Uh, a new date: " + dateTime.ToString());
    }

    private static void OnAtomicReadFileRequest(BacnetClient sender, BacnetAddress adr, byte invoke_id, bool is_stream, BacnetObjectId object_id, int position, uint count, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                if (object_id.type != BacnetObjectTypes.OBJECT_FILE) throw new Exception("File Reading on non file objects ... bah!");
                else if (object_id.instance != 0) throw new Exception("Don't know this file");

                //this is a test file for performance measuring
                int filesize = _storage.ReadPropertyValue(object_id, BacnetPropertyIds.PROP_FILE_SIZE);        //test file is ~10mb
                bool end_of_file = (position + count) >= filesize;
                count = (uint)Math.Min(count, filesize - position);
                int max_filebuffer_size = sender.GetFileBufferMaxSize();
                if (count > max_filebuffer_size && max_segments > 0)
                {
                    //create segmented message!!!
                }
                else
                {
                    count = (uint)Math.Min(count, max_filebuffer_size);     //trim
                }

                //fill file with bogus content 
                byte[] file_buffer = new byte[count];
                byte[] bogus = new byte[] { (byte)'F', (byte)'I', (byte)'L', (byte)'L' };
                for (int i = 0; i < count; i += bogus.Length)
                    Array.Copy(bogus, 0, file_buffer, i, Math.Min(bogus.Length, count - i));

                sender.ReadFileResponse(adr, invoke_id, sender.GetSegmentBuffer(max_segments), position, count, end_of_file, file_buffer);
                   
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_ATOMIC_READ_FILE, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnAtomicWriteFileRequest(BacnetClient sender, BacnetAddress adr, byte invoke_id, bool is_stream, BacnetObjectId object_id, int position, uint block_count, byte[][] blocks, int[] counts, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                if (object_id.type != BacnetObjectTypes.OBJECT_FILE) throw new Exception("File Reading on non file objects ... bah!");
                else if (object_id.instance != 0) throw new Exception("Don't know this file");

                //this is a test file for performance measuring
                //don't do anything with the content

                //adjust size though
                int filesize = _storage.ReadPropertyValue(object_id, BacnetPropertyIds.PROP_FILE_SIZE);
                int new_filesize = position + counts[0];
                if (new_filesize > filesize) _storage.WritePropertyValue(object_id, BacnetPropertyIds.PROP_FILE_SIZE, new_filesize);
                if (counts[0] == 0) _storage.WritePropertyValue(object_id, BacnetPropertyIds.PROP_FILE_SIZE, 0);      //clear file

                //send confirm
                sender.WriteFileResponse(adr, invoke_id, sender.GetSegmentBuffer(max_segments), position);
  
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_ATOMIC_WRITE_FILE, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnWritePropertyMultipleRequest(BacnetClient sender, BacnetAddress adr, byte invoke_id, IList<BacnetWriteAccessSpecification> properties, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                foreach (var prop in properties)
                    _storage.WritePropertyMultiple(prop.object_id, prop.values_refs);

                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROP_MULTIPLE, invoke_id);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROP_MULTIPLE, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnReadPropertyMultipleRequest(BacnetClient sender, BacnetAddress adr, byte invoke_id, IList<BacnetReadAccessSpecification> properties, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                IList<BacnetPropertyValue> value;
                List<BacnetReadAccessResult> values = new List<BacnetReadAccessResult>();
                foreach (BacnetReadAccessSpecification p in properties)
                {
                    if (p.propertyReferences.Count == 1 && p.propertyReferences[0].propertyIdentifier == (uint)BacnetPropertyIds.PROP_ALL)
                    {
                        if (!_storage.ReadPropertyAll(p.objectIdentifier, out value))
                        {
                            sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROP_MULTIPLE, invoke_id, BacnetErrorClasses.ERROR_CLASS_OBJECT, BacnetErrorCodes.ERROR_CODE_UNKNOWN_OBJECT);
                            return;
                        }
                    }
                    else
                        _storage.ReadPropertyMultiple(p.objectIdentifier, p.propertyReferences, out value);
                    values.Add(new BacnetReadAccessResult(p.objectIdentifier, value));
                }

                sender.ReadPropertyMultipleResponse(adr, invoke_id, sender.GetSegmentBuffer(max_segments), values); 

            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROP_MULTIPLE, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnWritePropertyRequest(BacnetClient sender, BacnetAddress adr, byte invoke_id, BacnetObjectId object_id, BacnetPropertyValue value, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                DeviceStorage.ErrorCodes code = _storage.WriteCommandableProperty(object_id, (BacnetPropertyIds)value.property.propertyIdentifier, value.value[0], value.priority);
                if (code == DeviceStorage.ErrorCodes.NotForMe)
                    code = _storage.WriteProperty(object_id, (BacnetPropertyIds)value.property.propertyIdentifier, value.property.propertyArrayIndex, value.value);

                if (code == DeviceStorage.ErrorCodes.Good)
                    sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROPERTY, invoke_id);
                else
                    sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROPERTY, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROPERTY, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnReadPropertyRequest(BacnetClient sender, BacnetAddress adr, byte invoke_id, BacnetObjectId object_id, BacnetPropertyReference property, BacnetMaxSegments max_segments)
    {
        lock (LockObject)
        {
            try
            {
                IList<BacnetValue> value;
                DeviceStorage.ErrorCodes code = _storage.ReadProperty(object_id, (BacnetPropertyIds)property.propertyIdentifier, property.propertyArrayIndex, out value);
                if (code == DeviceStorage.ErrorCodes.Good)
                    sender.ReadPropertyResponse(adr, invoke_id, sender.GetSegmentBuffer(max_segments), object_id, property, value);

                else
                    sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROPERTY, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROPERTY, invoke_id, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnWhoIs(BacnetClient sender, BacnetAddress adr, int low_limit, int high_limit)
    {
        lock (LockObject)
        {
            if (low_limit != -1 && _storage.DeviceId < low_limit) return;
            else if (high_limit != -1 && _storage.DeviceId > high_limit) return;
            else sender.Iam(_storage.DeviceId, SupportedSegmentation);
        }
    }
}