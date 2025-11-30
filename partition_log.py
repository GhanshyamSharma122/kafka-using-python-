'''
this file support is made for supporting the following functionality:
    1.append records along with maintaing montonically increasing offsets
    2.read starting at a given offset
    3.keeping small index file per segment for offset for file position lookup
    4.rotate segments when a segment file reaches a configured max size
    5.simple tests that append and read back

'''
import os
import struct
import json
from typing import List,Tuple
'''
record framing
the data will be stored in terms of bytes in big endian format
> represt big endian format
I represt unsigned 4 bytes integer
Q represent 8 byte offset
'''
RECORD_HEADER_FMT=">IQI"
RECORD_HEADER_SIZE=struct.calcsize(RECORD_HEADER_FMT)
class Segment:
    def __init__(self,dirpath:str,base_offset:int,max_segment_bytes:int=1_000_000):
        self.dirpath=dirpath
        os.makedirs(dirpath,exist_ok=True)
        self.base_offset=base_offset
        self.max_segment_bytes=max_segment_bytes
        self.log_path=os.path.join(dirpath,f"segment-{base_offset:020d}.log")
        self.index_path=os.path.join(dirpath,f"segment-{base_offset:020d}.idx")
        self._log_file=open(self.log_path,"ab+")
        self._index_file=open(self.index_path,"a+")
        self._log_file.seek(0,os.SEEK_END)
    def append(self,offset:int,payload:bytes)->int:
        payload_len=len(payload)
        header=struct.pack(">QI",offset,payload_len)
        total_len=len(header)+payload_len
        pos=self._log_file.tell()
        self._log_file.write(struct.pack(">I",total_len))
        self._log_file.write(header)
        self._log_file.write(payload)
        self._log_file.flush()
        self._index_file.write(f"{offset},{pos}\n")
        self._index_file.flush()
        return pos
    
    def read_from(self,start_offset:int,max_bytes:int)->List[Tuple[int,bytes]]:
        #read up to max bytes starting from start offset viz. inclusive. and then returns list of (offset ,payload)
        entries=[]
        self._index_file.seek(0)
        for line in self._index_file:
            off_str,pos_str=line.strip().split(',')
            off=int(off_str);pos=int(pos_str)
            entries.append((off,pos))
        results=[]
        bytes_read=0
        for off,pos in entries:
            if off<start_offset:
                continue
            with open(self.log_path,"rb") as f:
                f.seek(pos)
                raw_len=f.read(4)
                if len(raw_len)<4:
                    break
                (total_len,)=struct.unpack(">I",raw_len)
                body=f.read(total_len)
                if len(body)<total_len:
                    break
                offset,payload_len=struct.unpack(">QI",body[:12])
                payload=body[12:12+payload_len]
                results.append((offset,payload))
                bytes_read+=total_len+4
                if bytes_read>=max_bytes:
                    break
        return results
    def size(self)->int:
        self._log_file.flush()
        return os.path.getsize(self.log_path)
    def close(self)->int:
        self._log_file.close()
        self._index_file.close()
class PartitionLog:
    def __init__(self,base_dir:str,segment_max_bytes:int=1_000_000):
        self.base_dir=base_dir
        os.makedirs(base_dir,exist_ok=True)
        self.segment_max_bytes=segment_max_bytes
        self.segments=[]
        self._load_segments()
        self.next_offset=self._compute_next_offset()

    def _load_segments(self):
        seg_files=[f for f in os.listdir(self.base_dir) if f.endswith(".log")]
        offsets=[]
        for name in seg_files:
            try:
                base_off=int(name.split('-')[1].split('.')[0])
                offsets.append(base_off)
            except:
                continue
        offsets.sort()
        for off in offsets:
            segdir=self.base_dir
            seg=Segment(segdir,off,self.segment_max_bytes)
            self.segments.append(seg)
        if not self.segments:
            seg=Segment(self.base_dir,0,self.segment_max_bytes)
            self.segments.append(seg)
    def _compute_next_offset(self)->int:
        last_seg=self.segments[-1]
        try:
            with open(last_seg.index_path,"rb") as f:
                lines=[ln for ln in f.read().splitlines() if ln.strip()]
                if not lines:
                    return last_seg.base_offset
                last_line=lines[-1].decode()
                off=int(last_line.split(",")[0])
                return off+1
        except FileNotFoundError:
            return last_seg.base_offset
    def _current_segment(self)->Segment:
        return self.segments[-1]
    def append(self,payload:bytes)->int:
        offset=self.next_offset
        seg=self._current_segment()
        pos=seg.append(offset,payload)
        self.next_offset+=1
        #rotate if needed
        if seg.size()>=seg.max_segment_bytes:
            #seg.close()
            new_base=offset+1
            new_seg=Segment(self.base_dir,new_base,self.segment_max_bytes)
            self.segments.append(new_seg)
        return offset
    def read(self,start_offset:int,max_bytes:int=1024*1024)->List[Tuple[int,bytes]]:
        results=[]
        remaining=max_bytes
        for seg in self.segments:
            seg_end_estimate=seg.base_offset+1_000_000_000
            if seg.base_offset+1<=start_offset and seg!=self.segments[-1]:
                continue
            chunk=seg.read_from(start_offset,remaining)
            if chunk:
                results.extend(chunk)
                remaining-=sum((len(p) +RECORD_HEADER_SIZE) for _,p in chunk)
            if results:
                start_offset=results[-1][0]+1
            if remaining <=0:
                break
        return results
    def close(self):
        for s in self.segments:
            s.close()
if __name__=='__main__':
    import shutil
    test_dir='test_partition'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    pl=PartitionLog(test_dir,segment_max_bytes=1024)
    for i in range(200):
        msg=json.dumps({"i":i,"text":f"message {i}"}).encode('utf-8')
        off=pl.append(msg)
        if i%50==0:
            print("appended",off)
    print("next_offset:",pl.next_offset)
    out=pl.read(10,max_bytes=4096)
    print("read count starting offset 10:",len(out))
    print("first read:",json.loads(out[0][1].decode()))
    pl.close()




'''
the record header layout looks like this
[ record_len (4 bytes) ]
[ offset     (8 bytes) ]
[ payload_len (4 bytes) ]
[ payload bytes (payload_len bytes) ]
and after the header layout the acutal payload is there
'''

