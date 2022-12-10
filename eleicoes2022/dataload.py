import csv

class ObjectContext:
    
    def __init__(self):
        self.data = {}
    
    def create_for(self, name):
        assert name not in self.data
        self.data[name] = {}
    
    def exists(self, name, key):
        return key in self.data[name]
    
    def keys(self, name):
        return list(self.data[name].keys())
    
    def values(self, name):
        return list(self.data[name].values())
            
    def get(self, name, key):
        return self.data[name].get(key, None)
    
    def set(self, name, key, value):
        self.data[name][key] = value
        
class EntityMapper:
    
    def __init__(self, name, factory, key_fields, fields):        
        if isinstance(key_fields, str):
            key_fields = [key_fields]
        self.name = name
        self.factory = factory
        self.key_fields = key_fields
        self.fields = fields
        self.on_create_cbks = []
        self.composite_mapper = None
        self.obj_count = 0
        
    def extract_key(self, row):
        if len(self.key_fields) == 1:
            return getattr(row, self.key_fields[0])
        else:
            return tuple([ getattr(row, f) for f in self.key_fields ])
        
    def add_create_cbk(self, cbk):
        self.on_create_cbks.append(cbk)
        
    def map(self, ctx, row):
        key = self.extract_key(row)
        
        if len(self.key_fields) == 1:
            is_null = key is None
        else:
            is_null = not any(key)
        
        if is_null:
            return 
            
        if self.obj_count == 0:
            ctx.create_for(self.name)
        
        cached = ctx.exists(self.name, key)
        if cached:
            obj = ctx.get(self.name, key)
            return
        
        self.obj_count += 1
        
        args = list([ getattr(row, f) for f in self.fields ])
        obj = self.factory(*args)
        ctx.set(self.name, key, obj)
        for cbk in self.on_create_cbks:
            cbk(ctx, row, obj)

class CompositeEntityMapper:
    
    def __init__(self):
        self.mappers = {}
    
    def add_entity_mapper(self, mapper, create_cbks=None):
        if create_cbks is None:
            create_cbks = []
        
        self.mappers[mapper.name] = mapper
        mapper.composite_mapper = self
        for cbk in create_cbks:
            mapper.add_create_cbk(cbk)
            
    def map(self, ctx, row):
        for mapper in self.mappers.values():
            mapper.map(ctx, row)
