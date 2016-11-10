'''
Created on Jun 18, 2016

@author: arthurvaladares
'''
from __future__ import absolute_import

import logging
from collections import namedtuple
#from spacetime_local.frame import frame
from datamodel.common.datamodel import Quaternion, Vector3
from pcc.set import pcc_set
from pcc.attributes import dimension, primarykey
from pcc.subset import subset
from pcc.parameter import parameter

logger = logging.getLogger(__name__)
LOG_HEADER = "[DATAMODEL]"

class Capsule(object):
    def __init__(self, sname = None, dname = None):
        self.SourceName = sname
        self.DestinationName = dname

    def __json__(self):
        return self.__dict__

    @staticmethod
    def __decode__(dic):
        if dic:
            if 'SourceName' in dic and 'DestinationName' in dic:
                return Capsule(dic['SourceName'], dic['DestinationName'])
            else:
                raise Exception("Could not decode Capsule with dic %s" % dic)
        else:
            return None

@pcc_set
class SimulationNode(object):
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(Vector3)
    def Center(self):
        return self._Center

    @Center.setter
    def Center(self, value):
        self._Center = value

    @dimension(int)
    def Angle(self):
        return self._Angle

    @Angle.setter
    def Angle(self, value):
        self._Angle = value

    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(int)
    def Width(self):
        return self._Width

    @Width.setter
    def Width(self, value):
        self._Width = value

    @dimension(Capsule)
    def Rezcap(self):
        return self._Rezcap

    @Rezcap.setter
    def Rezcap(self, value):
        self._Rezcap = value

    def __init__(self):
        self.Center = Vector3(0,0,0)
        self.Angle = 0
        self.Name = ""
        self.Width = 0
        self.Rezcap = Capsule()

@pcc_set
class Road(object):
    def __init__(self):
        self.StartingPoint = Vector3(0,0,0)
        self.EndPoint = Vector3(0,0,0)
        self.Width = 0
        self.Type = None

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(Vector3)
    def StartingPoint(self):
        return self._StartPoint

    @StartingPoint.setter
    def StartingPoint(self, value):
        self._StartPoint = value

    @dimension(Vector3)
    def EndPoint(self):
        return self._EndPoint

    @EndPoint.setter
    def EndPoint(self, value):
        self._EndPoint = value

    @dimension(int)
    def Width(self):
        return self._Width

    @Width.setter
    def Width(self, value):
        self._Width = value

    @dimension(str)
    def Type(self):
        return self._Type

    @Type.setter
    def Type(self, value):
        self._Type = value

@pcc_set
class BusinessNode(SimulationNode):
    @dimension(int)
    def CustomersPerNode(self):
        return self._CustomersPerNode

    @CustomersPerNode.setter
    def CustomersPerNode(self, value):
        self._CustomersPerNode = value

    @dimension(int)
    def EmployeesPerNode(self):
        return self._EmployeesPerNode

    @EmployeesPerNode.setter
    def EmployeesPerNode(self, value):
        self._EmployeesPerNode = value

    @dimension(int)
    def PreferredBusinessTypes(self):
        return self._PreferredBusinessTypes

    @PreferredBusinessTypes.setter
    def PreferredBusinessTypes(self, value):
        self._PreferredBusinessTypes = value

    @dimension(int)
    def PeakEmployeeCount(self):
        return self._PeakEmployeeCount

    @PeakEmployeeCount.setter
    def PeakEmployeeCount(self, value):
        self._PeakEmployeeCount = value

    @dimension(int)
    def PeakCustomerCount(self):
        return self._PeakCustomerCount

    @PeakCustomerCount.setter
    def PeakCustomerCount(self, value):
        self._PeakCustomerCount = value

    def __init__(self):
        self.CustomersPerNode = 0
        self.EmployeesPerNode = 0
        self.PreferredBusinessTypes = 0
        self.PeakEmployeeCount = 0
        self.PeakCustomerCount = 0
        super(BusinessNode, self).__init__()

@pcc_set
class ResidentialNode(SimulationNode):
    @dimension(int)
    def ResidentsPerNode(self):
        return self._ResidentsPerNode

    @ResidentsPerNode.setter
    def ResidentsPerNode(self, value):
        self._ResidentsPerNode = value

    @dimension(int)
    def ResidentCount(self):
        return self._ResidentCount

    @ResidentCount.setter
    def ResidentCount(self, value):
        self._ResidentCount = value

    @dimension(list)
    def ResidenceList(self):
        return self._ResidenceList

    @ResidenceList.setter
    def ResidenceList(self, value):
        self._ResidenceList = value

    def __init__(self):
        self.ResidentsPerNode = 0
        self.ResidentCount = 0
        self.ResidenceList = []
        super(ResidentialNode, self).__init__()

class JobDescription(object):
    def __init__(self, salary = 0, flexhours = False, schedule = None):
        self.Salary = salary
        self.FlexibleHours = flexhours
        self.Schedule = schedule

    def __json__(self):
        return self.__dict__

    @staticmethod
    def __decode__(dic):
        if dic:
            if 'Salary' in dic and 'FlexibleHours' in dic and 'Schedule' in dic:
                return JobDescription(dic['Salary'], dic['FlexibleHours'], dic['Schedule'])
            else:
                raise Exception("Could not decode VehicleInfo with dic %s" % dic)
        else:
            return None


class VehicleInfo(object):
    def __init__(self, vname = None, vtype = None):
        self.VehicleName = vname
        self.VehicleType = vtype

    def __json__(self):
        return self.__dict__

    @staticmethod
    def __decode__(dic):
        if dic:
            if 'VehicleName' in dic and 'VehicleType' in dic:
                return VehicleInfo(dic['VehicleName'], dic['VehicleType'])
            else:
                raise Exception("Could not decode VehicleInfo with dic %s" % dic)
        else:
            return None

@pcc_set
class Person(object):
    def __init__(self):
        self.Name = None
        self.JobDescription = JobDescription()
        self.Preference = None
        self.Vehicle = VehicleInfo()
        self.EmployedBy = None

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(JobDescription)
    def JobDescription(self):
        return self._JobDescription

    @JobDescription.setter
    def JobDescription(self, value):
        self._JobDescription = value

    @dimension(str)
    def Preference(self):
        return self._Preference

    @Preference.setter
    def Preference(self, value):
        self._Preference = value

    @dimension(VehicleInfo)
    def Vehicle(self):
        return self._Vehicle

    @Vehicle.setter
    def Vehicle(self, value):
        self._Vehicle = value

    @dimension(BusinessNode)
    def EmployedBy(self):
        return self._EmployedBy

    @EmployedBy.setter
    def EmployedBy(self, value):
        self._EmployedBy = value

    @dimension(ResidentialNode)
    def LivesAt(self):
        return self._LivesAt

    @LivesAt.setter
    def LivesAt(self, value):
        self._LivesAt = value

@pcc_set
class MobdatVehicle(object):
    '''
    classdocs
    '''

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value
    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(str)
    def Type(self):
        return self._Type

    @Type.setter
    def Type(self, value):
        self._Type = value

    @dimension(str)
    def Route(self):
        return self._Route

    @Route.setter
    def Route(self, value):
        self._Route = value

    @dimension(str)
    def Target(self):
        return self._Target

    @Target.setter
    def Target(self, value):
        self._Target = value

    @dimension(Vector3)
    def Position(self):
        return self._Position

    @Position.setter
    def Position(self, value):
        self._Position = value

    @dimension(Vector3)
    def Velocity(self):
        return self._Velocity

    @Velocity.setter
    def Velocity(self, value):
        self._Velocity = value

    @dimension(Quaternion)
    def Rotation(self):
        return self._Rotation

    @Rotation.setter
    def Rotation(self, value):
        self._Rotation = value

    def __init__(self):
        self.Name = ""
        self.Position = Vector3(0,0,0)
        self.Route = ""
        self.Target = ""
        self.Velocity = Vector3(0,0,0)
        self.Rotation = Quaternion(0,0,0,0)
        self.Type = ""

@subset(MobdatVehicle)
class MovingVehicle(MobdatVehicle):
    @staticmethod
    def __query__(vehicles):  # @DontTrace
        return [c for c in vehicles if MovingVehicle.__predicate__(c)]

    @staticmethod
    def __predicate__(v):
        return v.Velocity != Vector3(0,0,0) or v.Position != (0,0,0)

@pcc_set
class PrimeNode(BusinessNode):
    @dimension(list)
    def Customers(self):
        return self._Customers

    @Customers.setter
    def Customers(self, value):
        self._Customers = value

    def __init__(self):
        self.Customers = []
        super(PrimeNode, self).__init__()

@parameter(Person)
@subset(BusinessNode)
class EmptyBusiness(BusinessNode):
    @staticmethod
    def query(bns, ppl):
        return [b for b in bns if EmptyBusiness.__predicate__(b, ppl)]

    @staticmethod
    def __predicate__(b, ppl):
        for p in ppl:
            if p.EmployedBy.ID == b.ID:
                    occupied = True
                    continue
            if not occupied:
                return True
            else:
                return False
