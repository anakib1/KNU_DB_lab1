import os
import pickle

def get_portion_of_file(fname, start, len):
    with open(fname, 'r') as f:
        f.seek(start)
        return f.read(len)

def write_portion_of_file(fname, start, data):
    with open(fname, 'r+') as f:
        f.seek(start)
        f.write(data)

class SizeException(Exception):
    pass

def to_max_size(thing, max_size):
    thing = str(thing)
    if len(thing) > max_size:
        raise SizeException('Length of fields over 20 is unsupported')
    return thing + ' ' * (max_size - len(thing))

MAX_ATTR_SIZE = 15
MAX_ID_SIZE = 5
NLLID = -1

LST_OF_ATTR_MASTER = ['name', 'age']
LST_OF_ATTR_SLAVE = ['title', 'duration']

TOTAL_LEN_MASTER = len(LST_OF_ATTR_MASTER) * MAX_ATTR_SIZE + 2 * MAX_ID_SIZE
TOTAL_LEN_SLAVE = len(LST_OF_ATTR_SLAVE) * MAX_ATTR_SIZE + 4 * MAX_ID_SIZE

MASTER_FILE = 'masters.txt'
SLAVE_FILE = 'slaves.txt'
index_m_FILE = 'index_m.txt'

class Entity:
    """
    <id><dict>
    """
    def __init__(self, id, str_fields, names, pos, file):

        self.id = id
        self.str_fields = str_fields
        self.key_names = names
        self.pos = pos
        self.file = file

    def upd(self):
        write_portion_of_file(self.file, self.pos, self.__str__())


    def __str__(self):
        ans = to_max_size(self.id, MAX_ID_SIZE)
        for key in self.key_names:
            ans += to_max_size(self.str_fields[key], MAX_ATTR_SIZE)
        return ans
    
    def from_str(self, s:str):
        self.id = int(s[:MAX_ID_SIZE])
        self.str_fields = dict()

        id = MAX_ID_SIZE
        for val in self.key_names:
            self.str_fields[val] = s[id:id+MAX_ATTR_SIZE]
            id += MAX_ATTR_SIZE
    
class Master(Entity):
    """
    Order : <id><dict...>
    """
    def __init__(self, id = NLLID, str_fields = {}, pos = 0, first_address = NLLID):
        super().__init__(id, str_fields, LST_OF_ATTR_MASTER, pos, MASTER_FILE)
        self.first_address = first_address
    
    def __str__(self):
        ans = super().__str__()
        ans += to_max_size(self.first_address, MAX_ID_SIZE)
        return ans

    def from_str(self, s: str):
        super().from_str(s)
        self.first_address = s[-MAX_ID_SIZE:]
    def from_pos(self, pos:int):
        self.from_str(get_portion_of_file(MASTER_FILE, pos, TOTAL_LEN_MASTER))
        self.pos = pos
    def nice(self):
        return f'Director:\n\nName: {self.str_fields["name"]}\nAge: {self.str_fields["age"]}\nId: {self.id}\n'
class Slave(Entity):
    """
    Order : <id><dict...><master_id>
    """
    def __init__(self, id = NLLID, str_fields = {}, pos = 0, master_id = NLLID, nxt_address = NLLID, prv_address = NLLID):
        super().__init__(id, str_fields, LST_OF_ATTR_SLAVE, pos, SLAVE_FILE)
        self.master_id = master_id
        self.nxt_address = nxt_address
        self.prv_address = prv_address
    
    def __str__(self):
        ans = super().__str__()
        ans += to_max_size(self.master_id, MAX_ID_SIZE)
        ans += to_max_size(self.prv_address, MAX_ID_SIZE)
        ans += to_max_size(self.nxt_address, MAX_ID_SIZE)
        return ans

    def from_str(self, s: str):
        super().from_str(s)
        self.master_id = int(s[-3 * MAX_ID_SIZE:-2 * MAX_ID_SIZE])
        self.prv_address = int(s[-2 * MAX_ID_SIZE:-1 * MAX_ID_SIZE])
        self.nxt_address = int(s[-MAX_ID_SIZE:])
    def nice(self):
        return f'Movie:\nDirector ID: {self.master_id}\nTitle: {self.str_fields["title"]}\nDuration: {self.str_fields["duration"]}\nId: {self.id}\n'
    def from_pos(self, pos:int):
        self.from_str(get_portion_of_file(SLAVE_FILE, pos, TOTAL_LEN_SLAVE))
        self.pos = pos

def get_int(msg = 'Input id: '):
    z = input(msg)
    while not z.isnumeric():
        print('Input number please')
        z = input(msg)
    return int(z)
        

def prompt_integer_in_table(table, xor = False, msg = 'Input id: '):
    id = get_int(msg)
    while not( (id in table.keys()) ^ xor):
        if xor: print('Id already used in DB!')
        else: print('No such id in DB')
        id = get_int()
    return id
def prompt_str(message):
    s = input(message)
    while len(s) > MAX_ATTR_SIZE:
        print(f'String too long! Maximum length if {MAX_ATTR_SIZE} characters!')
        s = input(message)
    return s

class DB:
    def __init__(self):
        self.index_m = dict()
        self.index_ms = dict()
        self.index_s = dict()

        self.cnt_m = 0
        self.cnt_s = 0

    
    def add_m(self, name, age, id):
        self.index_m[id] = self.cnt_m * TOTAL_LEN_MASTER 
        self.index_ms[id] = []

        master = Master(id, {'name' : name, 'age' : age}, self.index_m[id])
        master.upd()

        self.cnt_m += 1

    def add_s(self, title, duration, id, director):
        self.index_s[id] = self.cnt_s * TOTAL_LEN_SLAVE

        self.index_ms[director].append(id)

        slave = Slave(id, {'title' : title, 'duration' : duration},self.index_s[id], director )
        slave.upd()

        self.cnt_s += 1

    def master_by_id(self, id):
        m = Master()
        m.from_pos(self.index_m[id])
        return m

    def slave_by_id(self, id):
        s = Slave()
        s.from_pos(self.index_s[id])
        return s

    def get_m(self, id):

        master = self.master_by_id(id)

        return master.nice()
    
    def get_s(self, id):

        slave = self.slave_by_id(id)

        return slave.nice()

    def del_m(self, id):
        
        pos = self.index_m[id]    
        
        for sub in self.index_ms[id]:
            self.del_s(sub)
        
        self.index_ms.pop(id)
        self.index_m.pop(id)

        write_portion_of_file(MASTER_FILE, pos, ' ' * TOTAL_LEN_MASTER)

    def del_s(self, id):
        pos = self.index_s.pop(id)
        write_portion_of_file(SLAVE_FILE, pos, ' ' * TOTAL_LEN_SLAVE)

    def upd_m(self, id, field, val):

        master = self.master_by_id(id)
        if field == 'Name':
            master.str_fields['name'] = val
        else:
            master.str_fields['age'] = val

        master.upd()
    
    def upd_s(self, id, field, val):
        slave = self.slave_by_id(id)
        if field == 'Title':
            slave.str_fields['title'] = val
        else:
            slave.str_fields['duration'] = val

        slave.upd()
    def save_index_tables(self):
        with open('index_tables.pickle', 'wb') as handle:
            pickle.dump((self.index_m, self.index_s, self.index_ms), handle, protocol=pickle.HIGHEST_PROTOCOL)

    
    def read_data(self):
        files = list(os.listdir())
        if 'index_tables.pickle' in files:
            with open('index_tables.pickle', 'rb') as handle:
                x, y, z = pickle.load(handle)
                if isinstance(x, dict) and isinstance(y, dict) and isinstance(z, dict):
                    self.index_m = x
                    self.index_s = y
                    self.index_ms = z
                    print('LOADED INDEX TABLE SUCCESSFULLY. No need to load files')
                    return
        print('No index table found. Building from scratch')
        try:
            with open('masters.txt', 'r+') as f:
                sz = os.path.getsize('masters.txt')
                print(f'Masters file length: {sz} symbols')
                if sz % TOTAL_LEN_MASTER != 0:
                    print('Corrupted file. unable to retrieve data, overwriting')
                    f.truncate(0)
                for i in range(sz//TOTAL_LEN_MASTER):
                    f.seek(i * TOTAL_LEN_MASTER)
                    s = f.read(TOTAL_LEN_MASTER)
                    if s == ' ' * TOTAL_LEN_MASTER:
                        continue
                    master = Master()
                    master.from_pos(i * TOTAL_LEN_MASTER)
                    self.index_m[master.id] = i * TOTAL_LEN_MASTER
                    self.index_ms[master.id] = []
                
            with open('slaves.txt', 'r+') as f:
                sz = os.path.getsize('slaves.txt')
                print(f'Slaves file length: {sz} symbols')
                if sz % TOTAL_LEN_SLAVE != 0:
                    print('Corrupted file. unable to retrieve data, overwriting')
                    f.truncate(0)
                for i in range(sz//TOTAL_LEN_SLAVE):
                    f.seek(i * TOTAL_LEN_SLAVE)
                    s = f.read(TOTAL_LEN_SLAVE)
                    if s == ' ' * TOTAL_LEN_SLAVE:
                        continue
                    slave = Slave()
                    slave.from_pos(i * TOTAL_LEN_SLAVE)
                    self.index_s[slave.id] = i * TOTAL_LEN_SLAVE
                    self.index_ms[slave.master_id].append(slave.id)

            self.cnt_m = len(self.index_m)
            self.cnt_s = len(self.index_s)
            print('Successfully read DB file')
        except Exception as e:
            print('Internal error while reading file. Aborting read and initialising empty')
            print(e)
            self.__init__()
            


        
if __name__ == '__main__':
    print('Commands description:\n0: EXIT\n1: ADD DIRECTOR\n2: ADD MOVIE\n3: GET DIRECTOR\n4: GET MOVIE\n5: DEL DIRECTOR (and all movies)\n6: DEL MOVIE\n7: UPDATE DIRECTOR\n8: UPDATE MOVIE\n9: PRINT ALL DIRECTORS\n10: PRINT ALL MOVIES')
    db = DB()
    print('Do you want to load previous session?')
    tt = input()
    while not tt in ['Yes', 'No']:
        print('Input Yes or No')
        tt = input()
    if tt == 'Yes':
        db.read_data()
    else:
        f = open('masters.txt', 'w')
        f.close()
        f = open('slaves.txt', 'w')
        f.close()
    while True:
        x = get_int('Input command number: ')
        try:
            if x == 0:
                db.save_index_tables()
                print('Ok, GoodBye!')
                break
            elif x == 1:
                name = prompt_str('Input name of director: ')
                age = get_int('Input age of director: ')
                id = prompt_integer_in_table(db.index_m, True, 'Input the ID of director: ')
                db.add_m(name, age, id)
                print('Successfully created new director in DB')        
            elif x == 2:
                title = prompt_str('Input title of film: ')
                duration = get_int('Input duration of film in minutes: ')
                id = prompt_integer_in_table(db.index_s, True, 'Input the ID of film: ')
                dir_id = prompt_integer_in_table(db.index_m, msg = 'Input the (already existing) ID of film director: ')
                db.add_s(title, duration, id, dir_id)
                print('Successfully created new film in DB')
            elif x == 3:
                if(db.cnt_m == 0):
                    print('Directors DB is empty!')
                    continue
                id = prompt_integer_in_table(db.index_m, msg = 'Input existing director ID: ')
                print(db.get_m(id))
            elif x== 4:
                if(db.cnt_m == 0):
                    print('Directors DB is empty!')
                    continue
                if(db.cnt_s == 0):
                    print('Movies DB is empty!')
                    continue
                id = prompt_integer_in_table(db.index_m, msg = 'Input existing director ID: ')
                id2 = prompt_integer_in_table(db.index_s, msg = 'Input existing movie ID: ')
                print(db.get_m(id))
                print(db.get_s(id2))
            elif x==5:
                if(db.cnt_m == 0):
                    print('Directors DB is empty!')
                    continue
                id = prompt_integer_in_table(db.index_m, msg = 'Input existing director ID: ')
                things = db.del_m(id)
                print('Successfully deleted director from database')
            elif x == 6:
                if(db.cnt_m == 0):
                    print('Directors DB is empty!')
                    continue
                if(db.cnt_s == 0):
                    print('Movies DB is empty!')
                    continue
                id = prompt_integer_in_table(db.index_s, msg = 'Input existing film ID: ')
                things = db.del_s(id)
                print('Successfully deleted film from database')
            elif x == 7:
                if(db.cnt_m == 0):
                    print('Directors DB is empty!')
                    continue
                id = prompt_integer_in_table(db.index_m, msg = 'Input existing director ID: ')
                print(db.get_m(id))
                print('Write what field (Name, Age) you would like to change')
                s = input()
                while s not in {'Name', 'Age'}:
                    print('Write what field (Name, Age) you would like to change')
                    s = input()
                val = ''
                if s == 'Name':
                    val = input('Input new name: ')
                else:
                    val = get_int('Input new age: ')
                db.upd_m(id, s, val)
                print('Successfully cahnged director instance in DB')
            elif x == 8:
                if(db.cnt_m == 0):
                    print('Directors DB is empty!')
                    continue
                if(db.cnt_s == 0):
                    print('Movies DB is empty!')
                    continue
                id = prompt_integer_in_table(db.index_s, msg = 'Input existing film ID: ')
                print(db.get_s(id))
                print('Write what field (Title, Duration) you would like to change')
                s = input()
                while s not in {'Title', 'Duration'}:
                    print('Write what field (Title, Duratio) you would like to change')
                    s = input()
                val = ''
                if s == 'Title':
                    val = input('Input new title: ')
                else:
                    val = get_int('Input new duration: ')
                db.upd_s(id, s, val)
                print('Successfully cahnged movie instance in DB')
            elif x == 9:
                print('All directors: ')
                for x in db.index_m.keys():
                    print(db.master_by_id(x).nice())
            elif x == 10:
                print('All movies: ')
                for x in db.index_s.keys():
                    print(db.slave_by_id(x).nice())
            else:
                print('Wrong command!')
        except Exception as e:
            print('Internal error: ')
            print(e)