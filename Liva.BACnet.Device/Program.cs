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
using System.Globalization;
using System.IO.BACnet;
using System.IO.BACnet.Storage;

namespace Liva.BACnet.Device;

internal abstract class Program
{
    private static DeviceStorage _storage = new();
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
            Trace.Listeners.Add(
                new ConsoleTraceListener()); //Some of the classes are using the Trace/Debug to communicate logging. (As they should.) So let's catch those as well.

            lock (_storage)
            {
                _storage = DeviceStorage.Load("LIVA-EMS.xml");
                _storage.ChangeOfValue += m_storage_ChangeOfValue;
                _storage.ReadOverride += m_storage_ReadOverride;
            }

            //create udp service point
            var udpTransport = new BacnetIpUdpProtocolTransport(0xBAC0); //set to true to force "single socket" usage
            _ipServer = new BacnetClient(udpTransport);
            _ipServer.OnWhoIs += OnWhoIs;
            _ipServer.OnWhoHas += OnWhoHas;
            _ipServer.OnReadPropertyRequest += OnReadPropertyRequest;
            _ipServer.OnWritePropertyRequest += OnWritePropertyRequest;
            _ipServer.OnReadPropertyMultipleRequest += OnReadPropertyMultipleRequest;
            _ipServer.OnWritePropertyMultipleRequest += OnWritePropertyMultipleRequest;
            _ipServer.OnAtomicWriteFileRequest += OnAtomicWriteFileRequest;
            _ipServer.OnAtomicReadFileRequest += OnAtomicReadFileRequest;
            _ipServer.OnSubscribeCOV += OnSubscribeCOV;
            _ipServer.OnSubscribeCOVProperty += OnSubscribeCOVProperty;
            _ipServer.OnTimeSynchronize += OnTimeSynchronize;
            _ipServer.OnDeviceCommunicationControl += OnDeviceCommunicationControl;
            _ipServer.OnReinitializedDevice += OnReinitializedDevice;
            _ipServer.OnIam += OnIam;
            _ipServer.OnReadRange += OnReadRange;
            _ipServer.Start();

            //create pipe (MSTP) service point
            var pipeTransport = new BacnetPipeTransport("COM1003", true);
            var mstpTransport = new BacnetMstpProtocolTransport(pipeTransport, 0);
            mstpTransport.StateLogging =
                false; //if you enable this, it will display a lot of information about the StateMachine
            _mstpServer = new BacnetClient(mstpTransport);
            _mstpServer.OnWhoIs += OnWhoIs;
            _mstpServer.OnWhoHas += OnWhoHas;
            _mstpServer.OnReadPropertyRequest += OnReadPropertyRequest;
            _mstpServer.OnWritePropertyRequest += OnWritePropertyRequest;
            _mstpServer.OnReadPropertyMultipleRequest += OnReadPropertyMultipleRequest;
            _mstpServer.OnWritePropertyMultipleRequest += OnWritePropertyMultipleRequest;
            _mstpServer.OnAtomicWriteFileRequest += OnAtomicWriteFileRequest;
            _mstpServer.OnAtomicReadFileRequest += OnAtomicReadFileRequest;
            _mstpServer.OnSubscribeCOV += OnSubscribeCOV;
            _mstpServer.OnSubscribeCOVProperty += OnSubscribeCOVProperty;
            _mstpServer.OnTimeSynchronize += OnTimeSynchronize;
            _mstpServer.OnDeviceCommunicationControl += OnDeviceCommunicationControl;
            _mstpServer.OnReinitializedDevice += OnReinitializedDevice;
            _mstpServer.OnIam += OnIam;
            _mstpServer.OnReadRange += OnReadRange;
            _mstpServer.Start();

            //create pipe (PTP) service point
            var pipe2Transport = new BacnetPipeTransport("COM1004", true);
            var ptpTransport = new BacnetPtpProtocolTransport(pipe2Transport, true);
            ptpTransport.StateLogging = false; //if you enable this, it will display a lot of information
            _ptpServer = new BacnetClient(ptpTransport);
            _ptpServer.OnWhoIs += OnWhoIs;
            _ptpServer.OnWhoHas += OnWhoHas;
            _ptpServer.OnReadPropertyRequest += OnReadPropertyRequest;
            _ptpServer.OnWritePropertyRequest += OnWritePropertyRequest;
            _ptpServer.OnReadPropertyMultipleRequest += OnReadPropertyMultipleRequest;
            _ptpServer.OnWritePropertyMultipleRequest += OnWritePropertyMultipleRequest;
            _ptpServer.OnAtomicWriteFileRequest += OnAtomicWriteFileRequest;
            _ptpServer.OnAtomicReadFileRequest += OnAtomicReadFileRequest;
            _ptpServer.OnSubscribeCOV += OnSubscribeCOV;
            _ptpServer.OnSubscribeCOVProperty += OnSubscribeCOVProperty;
            _ptpServer.OnTimeSynchronize += OnTimeSynchronize;
            _ptpServer.OnDeviceCommunicationControl += OnDeviceCommunicationControl;
            _ptpServer.OnReinitializedDevice += OnReinitializedDevice;
            _ptpServer.OnIam += OnIam;
            _ptpServer.OnReadRange += OnReadRange;
            _ptpServer.Start();

            //display info
            Console.WriteLine("LIVA BACnet Device startet ...");
            Console.WriteLine("Udp service point - port: 0x" + udpTransport.SharedPort.ToString("X4") + "" +
                              (udpTransport.ExclusivePort != udpTransport.SharedPort
                                  ? " and 0x" + udpTransport.ExclusivePort.ToString("X4")
                                  : ""));
            Console.WriteLine("MSTP service point - name: \\\\.pipe\\" + pipeTransport.Name + ", source_address: " +
                              mstpTransport.SourceAddress + ", max_master: " + mstpTransport.MaxMaster +
                              ", max_info_frames: " + mstpTransport.MaxInfoFrames);
            Console.WriteLine("PTP service point - name: \\\\.pipe\\" + pipe2Transport.Name);
            Console.WriteLine("");

            //send greeting
            lock (_storage)
            {
                _ipServer.Iam(_storage.DeviceId, SupportedSegmentation);
                _mstpServer.Iam(_storage.DeviceId, SupportedSegmentation);
            }

            //endless loop of nothing
            Console.WriteLine("Press the ANY key to exit!");
            while (!Console.KeyAvailable)
            {
                //Endless loops of nothing are rather pointless, but I was too lazy to do anything fancy. 
                //And to be honest, it's not like it's sucking up a lot of system resources. 
                //If we'd made a GUI program or a Win32 service, we wouldn't have needed this. 
                Thread.Sleep(1000);
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

    private static BacnetLogRecord[] _trendSamples = Array.Empty<BacnetLogRecord>();

    private static byte[] GetEncodedTrends(uint start, int count, out BacnetResultFlags status)
    {
        status = BacnetResultFlags.NONE;
        start--; //position is 1 based

        if (start >= _trendSamples.Length || (start + count) > _trendSamples.Length)
        {
            Trace.TraceError("Trend data read overflow");
            return Array.Empty<byte>();
        }

        if (start == 0) status |= BacnetResultFlags.FIRST_ITEM;
        if ((start + count) >= _trendSamples.Length) status |= BacnetResultFlags.LAST_ITEM;
        else status |= BacnetResultFlags.MORE_ITEMS;

        var buffer = new System.IO.BACnet.Serialize.EncodeBuffer();
        for (var i = start; i < (start + count); i++)
        {
            System.IO.BACnet.Serialize.Services.EncodeLogRecord(buffer, _trendSamples[i]);
        }

        return buffer.ToArray();
    }

    private static void OnReadRange(BacnetClient sender, BacnetAddress adr, byte invokeId, BacnetObjectId objectId,
        BacnetPropertyReference property, System.IO.BACnet.Serialize.BacnetReadRangeRequestTypes requestType,
        uint position, DateTime time, int count, BacnetMaxSegments maxSegments)
    {
        if (objectId is { type: BacnetObjectTypes.OBJECT_TRENDLOG, instance: 0 })
        {
            //generate 100 samples
            if (!_trendSamples.Any())
            {
                _trendSamples = new BacnetLogRecord[100];
                var current = DateTime.Now.AddSeconds(-_trendSamples.Length);
                var rnd = new Random();
                for (var i = 0; i < _trendSamples.Length; i++)
                {
                    _trendSamples[i] = new BacnetLogRecord(BacnetTrendLogValueType.TL_TYPE_UNSIGN,
                        (uint)rnd.Next(0, 100), current, 0);
                    current = current.AddSeconds(1);
                }
            }

            //encode
            var applicationData = GetEncodedTrends(position, count, out var status);

            //send
            sender.ReadRangeResponse(adr, invokeId, sender.GetSegmentBuffer(maxSegments), objectId, property, status,
                (uint)count, applicationData, requestType, position);
        }
        else
            sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_RANGE, invokeId,
                BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
    }

    private static void OnIam(BacnetClient sender, BacnetAddress adr, uint deviceId, uint maxApdu,
        BacnetSegmentations segmentation, ushort vendorId)
    {
        //ignore Iams from other devices. (Also loopbacks)
    }

    /*****************************************************************************************************/
    // OnWhoHas by thamersalek
    static void OnWhoHas(BacnetClient sender, BacnetAddress adr, int lowLimit, int highLimit, BacnetObjectId objId,
        string objName)
    {
        lock (_storage)
        {
            if ((lowLimit == -1 && highLimit == -1) ||
                (_storage.DeviceId >= lowLimit && _storage.DeviceId <= highLimit))
            {
                var deviceId1 = new BacnetObjectId(BacnetObjectTypes.OBJECT_DEVICE, _storage.DeviceId);

                lock (_storage)
                {
                    if (!string.IsNullOrEmpty(objName))
                    {
                        foreach (var obj in _storage.Objects)
                        {
                            foreach (var p in obj.Properties)
                            {
                                if (p.Id == BacnetPropertyIds.PROP_OBJECT_NAME && // only Object Name property
                                    p.Tag == BacnetApplicationTags.BACNET_APPLICATION_TAG_CHARACTER_STRING) // it should be
                                {
                                    if (p.Value[0] == objName)
                                    {
                                        var objId2 = new BacnetObjectId(obj.Type, obj.Instance);
                                        sender.IHave(deviceId1, objId2, objName);
                                        return; // done
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        var obj = _storage.FindObject(objId);
                        if (obj != null)
                        {
                            foreach (var p in obj.Properties) // object name is mandatory
                            {
                                if (p is
                                    {
                                        Id: BacnetPropertyIds.PROP_OBJECT_NAME,
                                        Tag: BacnetApplicationTags.BACNET_APPLICATION_TAG_CHARACTER_STRING
                                    })
                                {
                                    sender.IHave(deviceId1, objId, p.Value[0]);
                                    return; // done
                                }
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
    private static void m_storage_ReadOverride(BacnetObjectId objectId, BacnetPropertyIds propertyId,
        uint arrayIndex, out IList<BacnetValue> value, out DeviceStorage.ErrorCodes status, out bool handled)
    {
        handled = true;
        value = Array.Empty<BacnetValue>();
        status = DeviceStorage.ErrorCodes.Good;

        if (objectId.type == BacnetObjectTypes.OBJECT_DEVICE && propertyId == BacnetPropertyIds.PROP_OBJECT_LIST)
        {
            if (arrayIndex == 0)
            {
                //object list count 
                value = new BacnetValue[]
                {
                    new(BacnetApplicationTags.BACNET_APPLICATION_TAG_UNSIGNED_INT, (uint)_storage.Objects.Length)
                };
            }
            else if (arrayIndex != System.IO.BACnet.Serialize.ASN1.BACNET_ARRAY_ALL)
            {
                //object list index 
                value = new BacnetValue[]
                {
                    new(BacnetApplicationTags.BACNET_APPLICATION_TAG_OBJECT_ID,
                        new BacnetObjectId(_storage.Objects[arrayIndex - 1].Type,
                            _storage.Objects[arrayIndex - 1].Instance))
                };
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
        else if (objectId.type == BacnetObjectTypes.OBJECT_DEVICE && objectId.instance == _storage.DeviceId &&
                 propertyId == BacnetPropertyIds.PROP_PROTOCOL_OBJECT_TYPES_SUPPORTED)
        {
            var v = new BacnetValue
            {
                Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_BIT_STRING
            };
            var b = new BacnetBitString();
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
            value = new[] { v };
        }
        else if (objectId.type == BacnetObjectTypes.OBJECT_DEVICE && objectId.instance == _storage.DeviceId &&
                 propertyId == BacnetPropertyIds.PROP_PROTOCOL_SERVICES_SUPPORTED)
        {
            var v = new BacnetValue
            {
                Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_BIT_STRING
            };
            var b = new BacnetBitString();
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
            value = new[] { v };
        }
        else if (objectId.type == BacnetObjectTypes.OBJECT_DEVICE && objectId.instance == _storage.DeviceId &&
                 propertyId == BacnetPropertyIds.PROP_SEGMENTATION_SUPPORTED)
        {
            var v = new BacnetValue
            {
                Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_ENUMERATED,
                Value = (uint)BacnetSegmentations.SEGMENTATION_BOTH
            };
            value = new[] { v };
        }
        else if (objectId.type == BacnetObjectTypes.OBJECT_DEVICE && objectId.instance == _storage.DeviceId &&
                 propertyId == BacnetPropertyIds.PROP_SYSTEM_STATUS)
        {
            var v = new BacnetValue
            {
                Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_ENUMERATED,
                Value = (uint)BacnetDeviceStatus.OPERATIONAL //can we be in any other mode I wonder?
            };
            value = new[] { v };
        }
        else if (objectId.type == BacnetObjectTypes.OBJECT_DEVICE && objectId.instance == _storage.DeviceId &&
                 propertyId == BacnetPropertyIds.PROP_ACTIVE_COV_SUBSCRIPTIONS)
        {
            var list = new List<BacnetValue>();
            foreach (var entry in Subscriptions)
            {
                foreach (var sub in entry.Value)
                {
                    //encode
                    //System.IO.BACnet.Serialize.EncodeBuffer buffer = new System.IO.BACnet.Serialize.EncodeBuffer();
                    var cov = new BacnetCOVSubscription
                    {
                        Recipient = sub.ReceiverAddress,
                        subscriptionProcessIdentifier = sub.SubscriberProcessIdentifier,
                        monitoredObjectIdentifier = sub.MonitoredObjectIdentifier,
                        monitoredProperty = sub.MonitoredProperty,
                        IssueConfirmedNotifications = sub.IssueConfirmedNotifications,
                        TimeRemaining = sub.Lifetime - (uint)(DateTime.Now - sub.Start).TotalMinutes,
                        COVIncrement = sub.CovIncrement
                    };
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
        else if (objectId is { type: BacnetObjectTypes.OBJECT_OCTETSTRING_VALUE, instance: 0 } &&
                 propertyId == BacnetPropertyIds.PROP_PRESENT_VALUE)
        {
            //this is our huge blob
            var v = new BacnetValue
            {
                Tag = BacnetApplicationTags.BACNET_APPLICATION_TAG_OCTET_STRING
            };
            var blob = new byte[2000];
            for (var i = 0; i < blob.Length; i++)
                blob[i] = (i % 2 == 0) ? (byte)'A' : (byte)'B';
            v.Value = blob;
            value = new[] { v };
        }
        else if (objectId.type == BacnetObjectTypes.OBJECT_GROUP &&
                 propertyId == BacnetPropertyIds.PROP_PRESENT_VALUE)
        {
            //get property list
            if (_storage.ReadProperty(objectId, BacnetPropertyIds.PROP_LIST_OF_GROUP_MEMBERS,
                    System.IO.BACnet.Serialize.ASN1.BACNET_ARRAY_ALL, out var properties) != DeviceStorage.ErrorCodes.Good)
            {
                value = new BacnetValue[]
                {
                    new(BacnetApplicationTags.BACNET_APPLICATION_TAG_ERROR,
                        new BacnetError(BacnetErrorClasses.ERROR_CLASS_DEVICE,
                            BacnetErrorCodes.ERROR_CODE_INTERNAL_ERROR))
                };
            }
            else
            {
                var values = new List<BacnetValue>();
                foreach (var p in properties)
                {
                    if (p.Value is BacnetReadAccessSpecification prop)
                    {
                        BacnetReadAccessResult result = new BacnetReadAccessResult
                        {
                            objectIdentifier = prop.objectIdentifier
                        };
                        var resultValues = new List<BacnetPropertyValue>();
                        foreach (var r in prop.propertyReferences)
                        {
                            var propValue = new BacnetPropertyValue
                            {
                                property = r
                            };
                            if (_storage.ReadProperty(prop.objectIdentifier, (BacnetPropertyIds)r.propertyIdentifier,
                                    r.propertyArrayIndex, out propValue.value) != DeviceStorage.ErrorCodes.Good)
                            {
                                propValue.value = new BacnetValue[]
                                {
                                    new(BacnetApplicationTags.BACNET_APPLICATION_TAG_ERROR,
                                        new BacnetError(BacnetErrorClasses.ERROR_CLASS_DEVICE,
                                            BacnetErrorCodes.ERROR_CODE_INTERNAL_ERROR))
                                };
                            }

                            resultValues.Add(propValue);
                        }

                        result.values = resultValues;
                        values.Add(new BacnetValue(BacnetApplicationTags.BACNET_APPLICATION_TAG_READ_ACCESS_RESULT,
                            result));
                    }
                }

                value = values;
            }
        }
        else
        {
            handled = false;
        }
    }

    private class Subscription
    {
        public readonly BacnetClient Receiver;
        public readonly BacnetAddress ReceiverAddress;
        public readonly uint SubscriberProcessIdentifier;
        public BacnetObjectId MonitoredObjectIdentifier;
        public readonly BacnetPropertyReference MonitoredProperty;
        public bool IssueConfirmedNotifications;
        public uint Lifetime;
        public DateTime Start;
        public readonly float CovIncrement;

        public Subscription(BacnetClient receiver, BacnetAddress receiverAddress, uint subscriberProcessIdentifier,
            BacnetObjectId monitoredObjectIdentifier, BacnetPropertyReference property,
            bool issueConfirmedNotifications, uint lifetime, float covIncrement)
        {
            this.Receiver = receiver;
            this.ReceiverAddress = receiverAddress;
            this.SubscriberProcessIdentifier = subscriberProcessIdentifier;
            this.MonitoredObjectIdentifier = monitoredObjectIdentifier;
            this.MonitoredProperty = property;
            this.IssueConfirmedNotifications = issueConfirmedNotifications;
            this.Lifetime = lifetime;
            this.Start = DateTime.Now;
            this.CovIncrement = covIncrement;
        }

        /// <summary>
        /// Returns the remaining subscription time. Negative values will imply that the subscription has to be removed.
        /// Value 0 *may* imply that the subscription doesn't have a timeout
        /// </summary>
        /// <returns></returns>
        public int GetTimeRemaining()
        {
            if (Lifetime == 0) return 0;
            else return (int)Lifetime - (int)(DateTime.Now - Start).TotalSeconds;
        }
    }

    private static void RemoveOldSubscriptions()
    {
        var toBeDeleted = new LinkedList<BacnetObjectId>();
        foreach (var entry in Subscriptions)
        {
            for (var i = 0; i < entry.Value.Count; i++)
            {
                if (entry.Value[i].GetTimeRemaining() < 0)
                {
                    Trace.TraceWarning("Removing old subscription: " + entry.Key);
                    entry.Value.RemoveAt(i);
                    i--;
                }
            }

            if (entry.Value.Count == 0)
                toBeDeleted.AddLast(entry.Key);
        }

        foreach (var objId in toBeDeleted)
        {
            Subscriptions.Remove(objId);
        }
    }

    private static Subscription? HandleSubscriptionRequest(BacnetClient sender, BacnetAddress adr, byte invokeId,
        uint subscriberProcessIdentifier, BacnetObjectId monitoredObjectIdentifier, uint propertyId,
        bool cancellationRequest, bool issueConfirmedNotifications, uint lifetime, float covIncrement)
    {
        //remove old leftovers
        RemoveOldSubscriptions();

        //find existing
        List<Subscription>? subs = null;
        Subscription? sub = null;
        if (Subscriptions.TryGetValue(monitoredObjectIdentifier, out var subscription))
        {
            subs = subscription;
            foreach (var s in subs)
            {
                if (Equals(s.Receiver, sender) && s.ReceiverAddress.Equals(adr) &&
                    s.SubscriberProcessIdentifier == subscriberProcessIdentifier &&
                    s.MonitoredObjectIdentifier.Equals(monitoredObjectIdentifier) &&
                    s.MonitoredProperty.propertyIdentifier == propertyId)
                {
                    sub = s;
                    break;
                }
            }
        }

        //cancel
        if (cancellationRequest && sub != null && subs != null)
        {
            subs.Remove(sub);
            if (subs.Count == 0)
                Subscriptions.Remove(sub.MonitoredObjectIdentifier);

            //send confirm
            sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV, invokeId);

            return null;
        }

        //create if needed
        if (sub == null)
        {
            sub = new Subscription(sender, adr, subscriberProcessIdentifier, monitoredObjectIdentifier,
                new BacnetPropertyReference((uint)BacnetPropertyIds.PROP_ALL,
                    System.IO.BACnet.Serialize.ASN1.BACNET_ARRAY_ALL), issueConfirmedNotifications, lifetime,
                covIncrement);
            if (subs == null)
            {
                subs = new List<Subscription>();
                Subscriptions.Add(sub.MonitoredObjectIdentifier, subs);
            }

            subs.Add(sub);
        }

        //update perhaps
        sub.IssueConfirmedNotifications = issueConfirmedNotifications;
        sub.Lifetime = lifetime;
        sub.Start = DateTime.Now;

        return sub;
    }

    private static void OnSubscribeCOV(BacnetClient sender, BacnetAddress adr, byte invokeId,
        uint subscriberProcessIdentifier, BacnetObjectId monitoredObjectIdentifier, bool cancellationRequest,
        bool issueConfirmedNotifications, uint lifetime, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                //create (will also remove old subscriptions)
                var sub = HandleSubscriptionRequest(sender, adr, invokeId, subscriberProcessIdentifier,
                    monitoredObjectIdentifier, (uint)BacnetPropertyIds.PROP_ALL, cancellationRequest,
                    issueConfirmedNotifications, lifetime, 0);

                //send confirm
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV, invokeId);

                //also send first values
                if (!cancellationRequest)
                {
                    ThreadPool.QueueUserWorkItem(_ =>
                    {
                        if (sub == null || !_storage.ReadPropertyAll(sub.MonitoredObjectIdentifier, out var values))
                            return;
                        if (!sender.Notify(adr, sub.SubscriberProcessIdentifier, _storage.DeviceId,
                                sub.MonitoredObjectIdentifier, (uint)sub.GetTimeRemaining(),
                                sub.IssueConfirmedNotifications, values))
                        {
                            Trace.TraceError("Couldn't send notify");
                        }
                    }, null);
                }
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnSubscribeCOVProperty(BacnetClient sender, BacnetAddress adr, byte invokeId,
        uint subscriberProcessIdentifier, BacnetObjectId monitoredObjectIdentifier,
        BacnetPropertyReference monitoredProperty, bool cancellationRequest, bool issueConfirmedNotifications,
        uint lifetime, float covIncrement, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                //create (will also remove old subscriptions)
                var sub = HandleSubscriptionRequest(sender, adr, invokeId, subscriberProcessIdentifier,
                    monitoredObjectIdentifier, (uint)BacnetPropertyIds.PROP_ALL, cancellationRequest,
                    issueConfirmedNotifications, lifetime, covIncrement);

                //send confirm
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV_PROPERTY,
                    invokeId);

                //also send first values
                if (!cancellationRequest)
                {
                    ThreadPool.QueueUserWorkItem(_ =>
                    {
                        if (sub != null)
                        {
                            _storage.ReadProperty(sub.MonitoredObjectIdentifier,
                                (BacnetPropertyIds)sub.MonitoredProperty.propertyIdentifier,
                                sub.MonitoredProperty.propertyArrayIndex, out var value);
                            var values = new List<BacnetPropertyValue>();
                            var tmp = new BacnetPropertyValue
                            {
                                property = sub.MonitoredProperty,
                                value = value
                            };
                            values.Add(tmp);
                            if (!sender.Notify(adr, sub.SubscriberProcessIdentifier, _storage.DeviceId,
                                    sub.MonitoredObjectIdentifier, (uint)sub.GetTimeRemaining(),
                                    sub.IssueConfirmedNotifications, values))
                                Trace.TraceError("Couldn't send notify");
                        }
                    }, null);
                }
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_SUBSCRIBE_COV_PROPERTY, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void m_storage_ChangeOfValue(DeviceStorage sender, BacnetObjectId objectId,
        BacnetPropertyIds propertyId, uint arrayIndex, IList<BacnetValue> value)
    {
        ThreadPool.QueueUserWorkItem(_ =>
        {
            lock (LockObject)
            {
                //remove old leftovers
                RemoveOldSubscriptions();

                //find subscription
                if (!Subscriptions.ContainsKey(objectId)) return;
                var subs = Subscriptions[objectId];

                //convert
                var values = new List<BacnetPropertyValue>();
                var tmp = new BacnetPropertyValue
                {
                    property = new BacnetPropertyReference((uint)propertyId, arrayIndex),
                    value = value
                };
                values.Add(tmp);

                //send to all
                foreach (var sub in subs)
                {
                    if (sub.MonitoredProperty.propertyIdentifier == (uint)BacnetPropertyIds.PROP_ALL ||
                        sub.MonitoredProperty.propertyIdentifier == (uint)propertyId)
                    {
                        //send notify
                        if (!sub.Receiver.Notify(sub.ReceiverAddress, sub.SubscriberProcessIdentifier,
                                _storage.DeviceId, sub.MonitoredObjectIdentifier, (uint)sub.GetTimeRemaining(),
                                sub.IssueConfirmedNotifications, values))
                            Trace.TraceError("Couldn't send notify");
                    }
                }
            }
        }, null);
    }

    private static void OnDeviceCommunicationControl(BacnetClient sender, BacnetAddress adr, byte invokeId,
        uint timeDuration, uint enableDisable, string password, BacnetMaxSegments maxSegments)
    {
        switch (enableDisable)
        {
            case 0:
                Trace.TraceInformation("Enable communication? Sure!");
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL,
                    invokeId);
                break;
            case 1:
                Trace.TraceInformation("Disable communication? ... smile and wave (ignored)");
                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL,
                    invokeId);
                break;
            case 2:
                Trace.TraceWarning("Disable initiation? I don't think so!");
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL,
                    invokeId, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
                break;
            default:
                Trace.TraceError("Now, what is this device_communication code: " + enableDisable + "!!!!");
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_DEVICE_COMMUNICATION_CONTROL,
                    invokeId, BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
                break;
        }
    }

    private static void OnReinitializedDevice(BacnetClient sender, BacnetAddress adr, byte invokeId,
        BacnetReinitializedStates state, string password, BacnetMaxSegments maxSegments)
    {
        Trace.TraceInformation("So you wanna reboot me, eh? Pfff! (" + state + ")");
        sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_REINITIALIZE_DEVICE, invokeId);
    }

    private static void OnTimeSynchronize(BacnetClient sender, BacnetAddress adr, DateTime dateTime, bool utc)
    {
        Trace.TraceInformation("Uh, a new date: " + dateTime.ToString(CultureInfo.InvariantCulture));
    }

    private static void OnAtomicReadFileRequest(BacnetClient sender, BacnetAddress adr, byte invokeId, bool isStream,
        BacnetObjectId objectId, int position, uint count, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                if (objectId.type != BacnetObjectTypes.OBJECT_FILE)
                    throw new Exception("File Reading on non file objects ... bah!");
                else if (objectId.instance != 0) throw new Exception("Don't know this file");

                //this is a test file for performance measuring
                var filesize = _storage.ReadPropertyValue(objectId, BacnetPropertyIds.PROP_FILE_SIZE); //test file is ~10mb
                var endOfFile = (position + count) >= filesize;
                count = (uint)Math.Min(count, filesize - position);
                var maxFileBufferSize = sender.GetFileBufferMaxSize();
                if (count > maxFileBufferSize && maxSegments > 0)
                {
                    //create segmented message!!!
                }
                else
                {
                    count = (uint)Math.Min(count, maxFileBufferSize); //trim
                }

                //fill file with bogus content 
                var fileBuffer = new byte[count];
                if (fileBuffer == null) throw new ArgumentNullException(nameof(fileBuffer));
                var bogus = new[] { (byte)'F', (byte)'I', (byte)'L', (byte)'L' };
                for (var i = 0; i < count; i += bogus.Length)
                    Array.Copy(bogus, 0, fileBuffer, i, Math.Min(bogus.Length, count - i));

                sender.ReadFileResponse(adr, invokeId, sender.GetSegmentBuffer(maxSegments), position, count,
                    endOfFile, fileBuffer);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_ATOMIC_READ_FILE, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnAtomicWriteFileRequest(BacnetClient sender, BacnetAddress adr, byte invokeId, bool isStream,
        BacnetObjectId objectId, int position, uint blockCount, byte[][] blocks, int[] counts, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                if (objectId.type != BacnetObjectTypes.OBJECT_FILE)
                    throw new Exception("File Reading on non file objects ... bah!");
                if (objectId.instance != 0) throw new Exception("Don't know this file");

                //this is a test file for performance measuring
                //don't do anything with the content

                //adjust size though
                var filesize = _storage.ReadPropertyValue(objectId, BacnetPropertyIds.PROP_FILE_SIZE);
                var newFilesize = position + counts[0];
                if (newFilesize > filesize)
                    _storage.WritePropertyValue(objectId, BacnetPropertyIds.PROP_FILE_SIZE, newFilesize);
                if (counts[0] == 0)
                    _storage.WritePropertyValue(objectId, BacnetPropertyIds.PROP_FILE_SIZE, 0); //clear file

                //send confirm
                sender.WriteFileResponse(adr, invokeId, sender.GetSegmentBuffer(maxSegments), position);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_ATOMIC_WRITE_FILE, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnWritePropertyMultipleRequest(BacnetClient sender, BacnetAddress adr, byte invokeId,
        IList<BacnetWriteAccessSpecification> properties, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                foreach (var prop in properties)
                    _storage.WritePropertyMultiple(prop.object_id, prop.values_refs);

                sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROP_MULTIPLE, invokeId);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROP_MULTIPLE, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnReadPropertyMultipleRequest(BacnetClient sender, BacnetAddress adr, byte invokeId,
        IList<BacnetReadAccessSpecification> properties, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                var values = new List<BacnetReadAccessResult>();
                foreach (var p in properties)
                {
                    IList<BacnetPropertyValue> value;
                    if (p.propertyReferences.Count == 1 &&
                        p.propertyReferences[0].propertyIdentifier == (uint)BacnetPropertyIds.PROP_ALL)
                    {
                        if (!_storage.ReadPropertyAll(p.objectIdentifier, out value))
                        {
                            sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROP_MULTIPLE,
                                invokeId, BacnetErrorClasses.ERROR_CLASS_OBJECT,
                                BacnetErrorCodes.ERROR_CODE_UNKNOWN_OBJECT);
                            return;
                        }
                    }
                    else
                        _storage.ReadPropertyMultiple(p.objectIdentifier, p.propertyReferences, out value);

                    values.Add(new BacnetReadAccessResult(p.objectIdentifier, value));
                }

                sender.ReadPropertyMultipleResponse(adr, invokeId, sender.GetSegmentBuffer(maxSegments), values);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROP_MULTIPLE, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnWritePropertyRequest(BacnetClient sender, BacnetAddress adr, byte invokeId,
        BacnetObjectId objectId, BacnetPropertyValue value, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                DeviceStorage.ErrorCodes code = _storage.WriteCommandableProperty(objectId,
                    (BacnetPropertyIds)value.property.propertyIdentifier, value.value[0], value.priority);
                if (code == DeviceStorage.ErrorCodes.NotForMe)
                    code = _storage.WriteProperty(objectId, (BacnetPropertyIds)value.property.propertyIdentifier,
                        value.property.propertyArrayIndex, value.value);

                if (code == DeviceStorage.ErrorCodes.Good)
                    sender.SimpleAckResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROPERTY, invokeId);
                else
                    sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROPERTY, invokeId,
                        BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_WRITE_PROPERTY, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnReadPropertyRequest(BacnetClient sender, BacnetAddress adr, byte invokeId,
        BacnetObjectId objectId, BacnetPropertyReference property, BacnetMaxSegments maxSegments)
    {
        lock (LockObject)
        {
            try
            {
                var code = _storage.ReadProperty(objectId,
                    (BacnetPropertyIds)property.propertyIdentifier, property.propertyArrayIndex, out var value);
                if (code == DeviceStorage.ErrorCodes.Good)
                    sender.ReadPropertyResponse(adr, invokeId, sender.GetSegmentBuffer(maxSegments), objectId,
                        property, value);

                else
                    sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROPERTY, invokeId,
                        BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
            catch (Exception)
            {
                sender.ErrorResponse(adr, BacnetConfirmedServices.SERVICE_CONFIRMED_READ_PROPERTY, invokeId,
                    BacnetErrorClasses.ERROR_CLASS_DEVICE, BacnetErrorCodes.ERROR_CODE_OTHER);
            }
        }
    }

    private static void OnWhoIs(BacnetClient sender, BacnetAddress adr, int low_limit, int high_limit)
    {
        lock (LockObject)
        {
            if (low_limit != -1 && _storage.DeviceId < low_limit) return;
            if (high_limit != -1 && _storage.DeviceId > high_limit) return;
            sender.Iam(_storage.DeviceId, SupportedSegmentation);
        }
    }
}