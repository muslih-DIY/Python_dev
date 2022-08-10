from functools import wraps

class with_connection:
    def select(function):
        @wraps(function)
        def inner(self,*args,**kwargs):
            #print(kwargs)
            con = kwargs.pop('con',self.con)
            kwargs['con']=con
            #print(self.con)
            #print(con)
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
                    if data is None:return 1
                    return data
        return inner
