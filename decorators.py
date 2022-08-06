from functools import wraps

class with_connection:
    def select(function):
        @wraps(function)
        def inner(self,*args,**kwargs):
            con = kwargs.pop('con',self.con)
            kwargs['con']=con
            with con.cursor() as cur:
                kwargs['cur']=cur
                try:
                    data = function(self,*args,**kwargs)
                except Exception as E:
                    self.error=str(E)
                    return 0
                else:
                    return data
        return inner
        
    def update(function):
        @wraps(function)
        def inner(self,*args,**kwargs):
            con = kwargs.pop('con',self.con)
            commit = kwargs.pop('commit',True)
            rollback = kwargs.pop('rollback',True)
            kwargs['con']=con
            with con.cursor() as cur:
                kwargs['cur']=cur
                try:
                    data = function(self,*args,**kwargs)
                except Exception as E:
                    if rollback:
                        self.con.rollback()
                    self.error=str(E)
                    return 0
                else:
                    if commit:
                        con.commit()
                    return data or 1
        return inner


def _with_connection(operation:str='put'):
    def with_connection_put(function):                
        @wraps(function)
        def inner(self,*args,**kwargs):
            con = kwargs.pop('con',self.con)
            commit = kwargs.pop('commit',True)
            rollback = kwargs.pop('rollback',True)
            kwargs['con']=con
            with con.cursor() as cur:
                kwargs['cur']=cur
                try:
                    data = function(self,*args,**kwargs)
                except Exception as E:
                    if rollback:
                        self.con.rollback()
                    self.error=str(E)
                    return 0
                else:
                    if commit:
                        con.commit()
                    return data or 1
        return inner
    
    def with_connection_get(function):                
        @wraps(function)
        def inner(self,*args,**kwargs):
            con = kwargs.pop('con',self.con)
            kwargs['con']=con
            with con.cursor() as cur:
                kwargs['cur']=cur
                try:
                    data = function(self,*args,**kwargs)
                except Exception as E:
                    self.error=str(E)
                    return 0
                else:
                    return data
        return inner
    if operation=='get':return with_connection_get
    return with_connection_put