package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/c2h5oh/datasize"
	treemap "github.com/igrmk/treemap/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"starlink-world/erigon-evm/common"
	common2 "starlink-world/erigon-evm/common2"
	"starlink-world/erigon-evm/crypto"
	kv2 "starlink-world/erigon-evm/kv/mdbx"
	"starlink-world/erigon-evm/log"
	"time"
)

var (
	databaseVerbosity = int(2)
	batchSizeStr      = "512M"
)

func openKV(label kv.Label, logger log.Logger, path string, exclusive bool) kv.RwDB {
	opts := kv2.NewMDBX(logger).Path(path).Label(label)
	if exclusive {
		opts = opts.Exclusive()
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts.MustOpen()
}

func openDB(path string, logger log.Logger, applyMigrations bool) kv.RwDB {
	label := kv.ChainDB
	db := openKV(label, logger, path, false)
	return db
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

var (
	fcode, farg, file *os.File
	wcode, warg       *bufio.Writer
	wcodeNum, wargNum int
	statename         = "get"
)

func checkfn1() {
	fileInfo, _ := fcode.Stat()
	if fileInfo.Size() >= 1024*1024*1024 {
		wcode.Flush()
		fcode.Close()
		wcodeNum++
		fcode, _ = os.Create(fmt.Sprintf("d:\\meta\\500\\1\\%sstate%04d.dat", statename, wcodeNum)) // bin with idx
		wcode = bufio.NewWriter(fcode)
	}
}

func comma(s string) string {
	n := len(s)
	if n <= 3 {
		return s
	}
	return comma(s[:n-3]) + "," + s[n-3:]
}

func StateGetAccount() { // 统计 账户数 账户存储大小 代码数 代码大小 状态数 状态大小
	// initial database
	logger := log.New()
	db := openDB("d:\\statedb", logger, true)
	defer db.Close()
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	wtx, err := db.BeginRw(context.Background())
	if err != nil {
		wtx.Rollback()
		panic(err)
	}
	wtx.CreateBucket("state")
	wtx.CreateBucket("account")
	//wtx.CreateBucket("code")
	acctfile, err := os.Create("d:\\meta\\500\\account.bin")
	statefile, err := os.Create("d:\\meta\\500\\state.bin")
	wacct := bufio.NewWriter(acctfile)
	wstate := bufio.NewWriter(statefile)

	b4 := make([]byte, 4)
	b2 := make([]byte, 2)
	b1 := make([]byte, 1)
	b20 := libcommon.Address{} // make([]byte, 20)
	b32 := libcommon.Hash{}    // make([]byte, 32)

	var bufread *bufio.Reader
	var tx uint16
	var blocknumber, bn uint32
	var seton bool = false
	var getnum, stats, accounts, txn, txn1, txn2, getn uint64
	//ma := make(map[libcommon.Address][]byte)
	//ms := make(map[libcommon.Address][]byte)
	// 遍历目录
	var (
		files []string
	)
	dirPth := "d:\\meta\\500\\" + statename + "\\"
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return
	}
	//PthSep := string(os.PathSeparator)
	for _, fi := range dir {
		if !fi.IsDir() { // 忽略目录
			files = append(files, dirPth+fi.Name())
		}
	}

	start := time.Now()
	start1 := time.Now()
	for _, fn := range files {
		fmt.Println(fn + " ")
		file, err := os.Open(fn)
		if err != nil {
			return
		}
		defer file.Close()
		bufread = bufio.NewReader(file) // bufio.NewReaderSize(file, 16384)
		seton = false                   // skip 05 begin
		// block
		for {
			n, err := io.ReadFull(bufread, b1)
			if n != 1 {
				break
			}
			if err == io.EOF { // io.EOF 文件的末尾 ?
				break
			}
			switch b1[0] {
			case 255: // block end
				//checkfn1()
				n, err = io.ReadFull(bufread, b4)
				blocknumber = binary.LittleEndian.Uint32(b4)
				io.ReadFull(bufread, b2)
				// 计算root
				seton = bn == blocknumber
				seton = true
				if !seton {
					//fmt.Println("blocknumber error! %d:%d", blocknumber, tx)
					//panic(1)
				}
				if seton {
					bn++
				}
				tx = 0
			case 254: // tx end
				io.ReadFull(bufread, b2)
				if binary.LittleEndian.Uint16(b2) != tx {
					fmt.Println("tx index error! %d:%d", blocknumber, tx)
					panic(1)
				}
				tx++
				txn++
				txn1++
			case 1, 5, 6: // GetBalance:  addr 20 len 1 blance  合并 nonce ?  5 AddBalance 6 SubBalance
				getnum++
				io.ReadFull(bufread, b20[:])
				//getaccountn(b20)
				len1, _ := bufread.ReadByte()
				tmp := make([]byte, len1)
				io.ReadFull(bufread, tmp)
				//if err1 := wtx.Put("account", b20[:], tmp); err1 != nil { // tmp  改成账户状态
				//	fmt.Printf("put err : %v", err1)
				//}
				//if _, ok := ma[b20]; !ok {
				//	ma[b20] = b1
				//	accounts++
				//}
				if a, _ := wtx.Has("account", b20[:]); !a {
					accounts++
					if err1 := wtx.Put("account", b20[:], tmp); err1 != nil { // tmp
						fmt.Printf("put err : %v", err1)
					}
					wacct.Write(b20[:])   // address
					wacct.Write(b4)       // nonce
					wacct.WriteByte(len1) // balance len
					wacct.Write(tmp)      // balance
				}
			case 2: // GetNonce: addr 20 nonce 4  skip?
				//getnum++
				io.ReadFull(bufread, b20[:])
				//getaccountn(b20)
				io.ReadFull(bufread, b4)
				//if err1 := wtx.Put("account", b20[:], b4); err1 != nil { // tmp
				//	fmt.Printf("put err : %v", err1)
				//}
				//if _, ok := ma[b20]; !ok {
				//	ma[b20] = b1
				//	accounts++
				//}
				//if a, _ := wtx.Has("account", b20[:]); !a {
				//	accounts++
				//	if err1 := wtx.Put("account", b20[:], b4); err1 != nil { //b4
				//		fmt.Printf("put err : %v", err1)
				//	}
				//}
			case 3: // GetCode, GetCodeSize, GetCodeHash: addr 20 code
				//getcoden(b20)
			case 4: // GetState: addr 20 key 32 value 32
				getnum++
				io.ReadFull(bufread, b20[:])
				io.ReadFull(bufread, b32[:])
				//s := b20.String() + b32.String()
				//getstaten(b20, b32)
				len1, _ := bufread.ReadByte()
				tmp := make([]byte, len1)
				io.ReadFull(bufread, tmp)
				//addr1 := make([]byte, 20)
				d := crypto.NewKeccakState()
				d.Write(b20.Bytes())
				d.Write(b32.Bytes())
				d.Read(b20[:])
				txn2++
				a, err := wtx.Has("state", b20[:])
				if err != nil {
					panic(err)
				}
				if !a {
					stats++
					if err1 := wtx.Put("state", b20[:], tmp); err1 != nil { // tmp
						fmt.Printf("put err : %v", err1)
					}
					wstate.Write(b20[:])   // state id address
					wstate.WriteByte(len1) // state len
					wstate.Write(tmp)      // state
				}
				//if _, ok := ms[b20]; !ok {
				//	ms[b20] = b1
				//	stats++
				//}
				////logWriter.WriteString(fmt.Sprintf("%6d:%5d GetState addr %20x key %32x len %5d val %32x\r\n", blocknumber, i, b20, b32, len1, tmp))
			}
			getn++
			if time.Now().Sub(start1) > time.Second {
				fmt.Printf("%s %8d:%5d tx %6d txs %12s get %7d getn %13s accounts %11s states %11s codes %5d %v\r", fn, blocknumber, tx, txn1,
					comma(fmt.Sprintf("%d", txn)), txn2,
					comma(fmt.Sprintf("%d", getn)),
					comma(fmt.Sprintf("%d", accounts)), comma(fmt.Sprintf("%d", stats)), 0, common.PrettyDuration(time.Since(start)))
				txn1 = 0
				txn2 = 0
				start1 = time.Now()
			}
		}
		file.Close()
		//wtx.Commit()
	}
	fmt.Printf("%8d:%5d tx %5d txs %12s get %6d getn %13s accounts %11s states %11s codes %5d %v\r", blocknumber, tx, txn1,
		comma(fmt.Sprintf("%d", txn)), txn2,
		comma(fmt.Sprintf("%d", getn)),
		comma(fmt.Sprintf("%d", accounts)), comma(fmt.Sprintf("%d", stats)), 0, common.PrettyDuration(time.Since(start)))
	//wtx.Rollback()wtx
	wacct.Flush()
	wstate.Flush()
	db.Close()
	acctfile.Close()
	statefile.Close()
}

func test() {
	start0 := time.Now()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("time %v alloc %s sys %s\n", common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
	//// 712710891
	ms1 := make(map[uint32][32]byte, 712710891)
	runtime.ReadMemStats(&m)
	fmt.Printf("time %v alloc %s sys %s\n", common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
	for i := 0; i < 712710891; i++ {
		if i%10000000 == 0 {
			fmt.Printf("put %9d  used: %s\n", i, time.Now().Sub(start0))
		}
		ms1[uint32(i)] = [32]byte{1}
	}
	runtime.ReadMemStats(&m)
	fmt.Printf("time %v alloc %s sys %s\n", common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())

	ms1 = nil
	runtime.ReadMemStats(&m)
	fmt.Printf("time %v alloc %s sys %s\n", common.PrettyDuration(time.Since(start0)), common.StorageSize(m.HeapAlloc).String(), common.StorageSize(m.Sys).String())

	logger := log.New()
	db := openDB("d:\\dbtest", logger, true)
	defer db.Close()
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	wtx, err := db.BeginRw(context.Background())
	if err != nil {
		wtx.Rollback()
		panic(err)
	}
	wtx.CreateBucket("test")
	b4 := make([]byte, 4)
	b32 := make([]byte, 32)
	start := time.Now() // 280s
	for i := 0; i < 712710891; i++ {
		if i%10000000 == 0 {
			fmt.Printf("put %9d  used: %s\n", i, time.Now().Sub(start))
		}
		binary.LittleEndian.PutUint32(b4, uint32(i))
		if err := wtx.Put("test", b4, b32); err != nil {
			fmt.Printf("put err : %v", err)
		}
	}
	fmt.Printf("put 712710891 used: %s\r\n", time.Now().Sub(start))
	i, err := wtx.BucketSize("test")

	fmt.Printf("BucketSize: %d\r\n", i)

	start = time.Now()
	for i := 10000000; i < 30000000; i++ {
		key := fmt.Sprintf("%9d", i)
		//		val := fmt.Sprintf("%9d", i+100000000)
		if _, err := wtx.GetOne("test", []byte(key)); err != nil {
			fmt.Printf("put err : %v", err)
		} else {
			//if strings.Compare(val, fmt.Sprintf("%9d", i+100000000)) != 0 {
			//	fmt.Printf("get err : %v", val)
			//}
		}
	}
	fmt.Printf("get 1000w used: %s\r\n", time.Now().Sub(start))
	//put 1000w used: 4.0126907s
	//get 1000w used: 3.1870853s
	test := make(map[string]string)
	start = time.Now()
	for i := 10000000; i < 30000000; i++ {
		key := fmt.Sprintf("%9d", i)
		val := fmt.Sprintf("%9d", i+1000000)
		test[key] = val
	}
	fmt.Printf("put 1000w used: %s\r\n", time.Now().Sub(start))

	start = time.Now()
	for i := 10000000; i < 30000000; i++ {
		key := fmt.Sprintf("%9d", i)
		_ = test[key]
	}
	fmt.Printf("get 1000w used: %s\r\n", time.Now().Sub(start))
	wtx.Rollback()
	db.Close()

}

// d:\statedb\state.bin
func test1() {
	start0 := time.Now()
	//var m runtime.MemStats
	//runtime.ReadMemStats(&m)
	//fmt.Printf("time %v alloc %s sys %s\n", common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
	////// 712710891
	tr := treemap.New[string, []byte]()
	//ms1 := make(map[libcommon.Address][]byte, 737157850)
	// open file
	file, err := os.Open("d:\\statedb\\state.bin")
	if err != nil {
		return
	}
	b20 := make([]byte, 20)
	stater := bufio.NewReader(file) // bufio.NewReaderSize(file, 16384)
	var n int64
	for {
		n++
		_, err := io.ReadFull(stater, b20[:])
		if err == io.EOF { // io.EOF 文件的末尾 ?
			break
		}
		//if n > 1000000 {
		//	break
		//}
		len1, _ := stater.ReadByte()
		tmp := make([]byte, len1)
		io.ReadFull(stater, tmp)
		tr.Set(string(b20), tmp)
		if n%1000000 == 0 {
			fmt.Printf("put %d used: %v\r\n", n, time.Now().Sub(start0))
		}
	}
	file.Close()
	//
	statefile, _ := os.Create("d:\\statedb\\state1.bin")
	wstate := bufio.NewWriter(statefile)
	for it := tr.Iterator(); it.Valid(); it.Next() {
		s := it.Key()
		wstate.Write([]byte(s))                 // address
		wstate.WriteByte(byte(len(it.Value()))) // val len
		wstate.Write(it.Value())                // val
	}
	wstate.Flush()
	statefile.Close()
	//runtime.ReadMemStats(&m)
	//fmt.Printf("time %v alloc %s sys %s\n", common.PrettyDuration(time.Since(start0)), common.StorageSize(m.Alloc).String(), common.StorageSize(m.Sys).String())
}

func test2() {
	start0 := time.Now()
	logger := log.New()
	dst := kv2.NewMDBX(logger).Path("d:\\dbtest").MustOpen()
	ctx, _ := common2.RootContext()
	wTx, err1 := dst.BeginRw(ctx)
	if err1 != nil {
		return
	}
	defer wTx.Rollback()
	bucket := "state"
	wTx.CreateBucket(bucket)
	c, err := wTx.RwCursor(bucket)
	//b2 := make([]byte, 2)
	// open file
	file, err := os.Open("d:\\statedb\\state1.bin")
	if err != nil {
		return
	}
	defer file.Close()
	b20 := make([]byte, 20)
	stater := bufio.NewReader(file) // bufio.NewReaderSize(file, 16384)
	var n int64
	for {
		n++
		_, err := io.ReadFull(stater, b20)
		if err == io.EOF { // io.EOF 文件的末尾 ?
			break
		}
		//io.ReadFull(stater, b2)
		//len1 := binary.LittleEndian.Uint16(b2)
		len1, _ := stater.ReadByte()
		tmp := make([]byte, len1)
		io.ReadFull(stater, tmp)
		if err = c.Append(b20, tmp); err != nil {
			panic(err)
		}

		//if err := wtx.Put("state", b20, tmp); err != nil {
		//	fmt.Printf("put err : %v", err)
		//}
		if n%1000000 == 0 {
			i, _ := wTx.BucketSize(bucket)
			fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
		}
	}
	i, _ := wTx.BucketSize(bucket)
	err = wTx.Commit()
	if err != nil {
		return
	}
	fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
}

func test3() {
	start0 := time.Now()
	tr := treemap.New[string, []byte]()
	logger := log.New()
	dst := kv2.NewMDBX(logger).Path("d:\\dbtest").MustOpen()
	ctx, _ := common2.RootContext()
	wTx, err1 := dst.BeginRw(ctx)
	if err1 != nil {
		return
	}
	defer wTx.Rollback()
	bucket := "state"
	wTx.CreateBucket(bucket)
	c, err := wTx.RwCursor(bucket)
	file, err := os.Open("d:\\statedb\\state.bin")
	if err != nil {
		return
	}
	defer file.Close()
	b20 := make([]byte, 20)
	stater := bufio.NewReader(file) // bufio.NewReaderSize(file, 16384)
	var n int64
	for {
		n++
		_, err := io.ReadFull(stater, b20)
		if err == io.EOF { // io.EOF 文件的末尾 ?
			break
		}
		len1, _ := stater.ReadByte()
		tmp := make([]byte, len1)
		io.ReadFull(stater, tmp)
		tr.Set(string(b20), tmp)
		if n%1000000 == 0 {
			i, _ := wTx.BucketSize(bucket)
			fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
			for it := tr.Iterator(); it.Valid(); it.Next() {
				s := it.Key()
				if err = c.Append([]byte(s), it.Value()); err != nil {
					panic(err)
				}
			}
			tr.Clear()
			c.Close()
			c, err = wTx.RwCursor(bucket)
			if err != nil {
				return
			}
		}
	}
	i, _ := wTx.BucketSize(bucket)
	for it := tr.Iterator(); it.Valid(); it.Next() {
		s := it.Key()
		if err = c.Append([]byte(s), it.Value()); err != nil {
			panic(err)
		}
	}
	err = wTx.Commit()
	if err != nil {
		return
	}
	fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
}

func sortw() {
	start0 := time.Now()
	tr := treemap.New[string, []byte]()
	logger := log.New()
	dst := kv2.NewMDBX(logger).Path("d:\\dbtest1").MustOpen()
	ctx, _ := common2.RootContext()
	wTx, err1 := dst.BeginRw(ctx)
	if err1 != nil {
		return
	}
	defer wTx.Rollback()
	bucket := "state"
	wTx.CreateBucket(bucket)
	c, err := wTx.RwCursor(bucket)
	file, err := os.Open("d:\\statedb\\state.bin")
	if err != nil {
		return
	}
	defer file.Close()
	b20 := make([]byte, 20)
	stater := bufio.NewReader(file) // bufio.NewReaderSize(file, 16384)
	var n int64
	for {
		n++
		_, err := io.ReadFull(stater, b20)
		if err == io.EOF { // io.EOF 文件的末尾 ?
			break
		}
		len1, _ := stater.ReadByte()
		tmp := make([]byte, len1)
		io.ReadFull(stater, tmp)
		tr.Set(string(b20), tmp)
		if n%1000000 == 0 {
			i, _ := wTx.BucketSize(bucket)
			fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
		}
		if n%10000000 == 0 {
			if n != 0 {
				for it := tr.Iterator(); it.Valid(); it.Next() {
					s := it.Key()
					if err = c.Put([]byte(s), it.Value()); err != nil {
						panic(err)
					} // 无法全部有序
				}
				tr.Clear()
				c.Close()
				wTx.Commit() // 提交了就要重建
				wTx, err1 = dst.BeginRw(ctx)
				if err1 != nil {
					return
				}
				c, err = wTx.RwCursor(bucket)
				if err != nil {
					return
				}
			}
		}
	}
	i, _ := wTx.BucketSize(bucket)
	for it := tr.Iterator(); it.Valid(); it.Next() {
		s := it.Key()
		if err = c.Put([]byte(s), it.Value()); err != nil {
			panic(err)
		}
	}
	err = wTx.Commit()
	if err != nil {
		return
	}
	fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
}

func dbcopy() {
	start0 := time.Now()
	logger := log.New()
	var tsize, fsize uint64
	src, err := kv2.Open("d:\\erigon\\chaindata", logger, true)
	ctx, _ := common2.RootContext()
	rTx, err := src.BeginRo(ctx)
	defer rTx.Rollback()
	if err != nil {
		return
	}
	migrator, ok := rTx.(kv.BucketMigrator)
	if !ok {
		return
	}
	Buckets, err := migrator.ListBuckets()
	for _, Bucket := range Buckets {
		size, _ := rTx.BucketSize(Bucket)
		tsize += size
		Cursor, _ := rTx.Cursor(Bucket)
		count, _ := Cursor.Count()
		Cursor.Close()
		if "BlockTransaction" == Bucket {
			fsize += size
		}
		if "Header" == Bucket {
			fsize += size
		}
		if "BlockBody" == Bucket {
			fsize += size
		}
		if "Receipt" == Bucket {
			fsize += size
		}
		if "TxSender" == Bucket {
			fsize += size
		}
		fmt.Printf("%30v count %10d size: %d\r\n", Bucket, count, size>>20)
	}
	fmt.Printf("total %d feezer %d used: %v\r\n", tsize>>20, fsize>>20, time.Now().Sub(start0))

	//dst := kv2.NewMDBX(logger).Path("d:\\dbtest1").MustOpen()
	//wTx, err := dst.BeginRw(ctx)
	//if err != nil {
	//	return
	//}
	//defer wTx.Rollback()
	////wTx.Commit()
	//dst.Close()

	src.Close()
	//rTx.BucketSize()
	//bucket := "state"
	//wTx.CreateBucket(bucket)
	//c, err := wTx.RwCursor(bucket)
	//file, err := os.Open("d:\\statedb\\state.bin")
	//if err != nil {
	//	return
	//}
	//defer file.Close()
	//b20 := make([]byte, 20)
	//stater := bufio.NewReader(file) // bufio.NewReaderSize(file, 16384)
	//var n int64
	//for {
	//	n++
	//	_, err := io.ReadFull(stater, b20)
	//	if err == io.EOF { // io.EOF 文件的末尾 ?
	//		break
	//	}
	//	len1, _ := stater.ReadByte()
	//	tmp := make([]byte, len1)
	//	io.ReadFull(stater, tmp)
	//	tr.Set(string(b20), tmp)
	//	if n%1000000 == 0 {
	//		i, _ := wTx.BucketSize(bucket)
	//		fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
	//	}
	//	if n%10000000 == 0 {
	//		if n != 0 {
	//			for it := tr.Iterator(); it.Valid(); it.Next() {
	//				s := it.Key()
	//				if err = c.Put([]byte(s), it.Value()); err != nil {
	//					panic(err)
	//				} // 无法全部有序
	//			}
	//			tr.Clear()
	//			c.Close()
	//			wTx.Commit() // 提交了就要重建
	//			wTx, err1 = dst.BeginRw(ctx)
	//			if err1 != nil {
	//				return
	//			}
	//			c, err = wTx.RwCursor(bucket)
	//			if err != nil {
	//				return
	//			}
	//		}
	//	}
	//}
	//i, _ := wTx.BucketSize(bucket)
	//for it := tr.Iterator(); it.Valid(); it.Next() {
	//	s := it.Key()
	//	if err = c.Put([]byte(s), it.Value()); err != nil {
	//		panic(err)
	//	}
	//}
	//err = wTx.Commit()
	//if err != nil {
	//	return
	//}
	//fmt.Printf("put %d,size %d used: %v\r\n", n, i>>20, time.Now().Sub(start0))
}

func main() {
	//StateGetAccount()
	//test()
	//test3()
	//test2()
	//sortw()
	dbcopy()
}
