package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

func ExecutePipeline(jobs ...job) {
	count := len(jobs)
	channelList := make([]chan interface{}, count + 1)
	for i := 0; i < count + 1; i++  {
		channelList[i] = make(chan interface{}, 100)
	}

	wg := &sync.WaitGroup{}
	for jobNumber, jobFunc := range jobs {
		wg.Add(1)
		go func(jobNumber int, jobFunc job, channelList []chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()

			chanelIn := channelList[jobNumber]
			chanelOut := channelList[jobNumber + 1]

			jobFunc(chanelIn, chanelOut)
			close(chanelOut)
		} (jobNumber, jobFunc, channelList, wg)
	}
	wg.Wait()

}

func SingleHash(in, out chan interface{}) {
	mutexMd5 := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for val := range in {
		wg.Add(1)
		data := fmt.Sprintf("%v", val)
		fmt.Println(data, " SingleHash data ", data)
		go func(data string,  group *sync.WaitGroup, mutexMd5 *sync.Mutex) {
			defer group.Done()

			wg := &sync.WaitGroup{}
			wg.Add(2)

			crc32md5 := ""
			crc32 := ""

			go func (data string, crc32md5 *string, wg *sync.WaitGroup, mutexMd5 *sync.Mutex) {
				defer wg.Done()

				fmt.Println(data, " SingleHash md5 ")
				mutexMd5.Lock()
				md5 := DataSignerMd5(data)
				mutexMd5.Unlock()
				fmt.Println(data, " SingleHash md5(data) ", md5)
				*crc32md5 = DataSignerCrc32(md5)
				fmt.Println(data, " SingleHash crc32(md5(data)) ", crc32md5)
			} (data, &crc32md5, wg, mutexMd5)

			go func (data string, crc32 *string, wg *sync.WaitGroup) {
				defer wg.Done()

				*crc32 = DataSignerCrc32(data)
				fmt.Println(data, " SingleHash crc32(data) ", crc32)
			} (data, &crc32, wg)

			wg.Wait()

			fmt.Println(data, " SingleHash result ", crc32 + "~" + crc32md5)

			out <- crc32 + "~" + crc32md5
		} (data, wg, mutexMd5)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{})  {
	fmt.Println("MultiHash")

	wg := &sync.WaitGroup{}
	for val := range in {
		wg.Add(1)
		data := fmt.Sprintf("%v", val)

		go func (data string, wg *sync.WaitGroup) {
			defer wg.Done()
			wgHash := &sync.WaitGroup{}
			var hash [6]string
			for i := 0; i < 6; i++ {
				wgHash.Add(1)
				go func(i int, data string, hash *[6]string, group *sync.WaitGroup) {
					defer group.Done()
					crc32 := DataSignerCrc32(fmt.Sprintf("%v", i) + data)
					fmt.Println(data, " MultiHash crc32(th+step1) ", i, crc32)
					hash[i] = crc32

				} (i, data, &hash, wgHash)
			}

			wgHash.Wait()
			out <- hash[0] + hash[1] + hash[2] + hash[3] + hash[4] + hash[5]
		} (data, wg)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{})  {
	fmt.Println("CombineResults")
	result := []string{}

	for val := range in {
		fmt.Println("sort CombineResults !!!!!!! ", val)
		result = append(result, fmt.Sprintf("%v",val))
	}
	sort.Strings(result)
	out <- fmt.Sprintf(strings.Join(result, "_"))
}

func main()  {
	//inputData := []int{0, 1}
	inputData := []int{0, 1, 1, 2, 3, 5, 8}

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataRaw := <-in
			data, ok := dataRaw.(string)
			if !ok {
				fmt.Println("cant convert result data to string")
			}
			fmt.Println(data)
		}),
	}
	start := time.Now()

	ExecutePipeline(hashSignJobs...)

	end := time.Since(start)


	fmt.Println("time", end.Seconds())
}