from abc import ABC,abstractmethod

class DbWrapBase(ABC):
    
    @abstractmethod
    def close(self):
        pass

    def is_connected(self):
        return NotImplemented
    
    def re_connect_if_not(self):
        return NotImplemented
    
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod    
    def dict_insert(self,values:dict,table:str,cur,con):
        pass
    
    @abstractmethod  
    def execute(self,query,cur,con):
        pass

    @abstractmethod  
    def execute_many(self,query,dataset,cur,con):
        pass

    @abstractmethod
    def select(self,query,cur,con,rtype=None,header=0):
        pass
