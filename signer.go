package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	fmt.Println("ExecutePipeline")

	in := make(chan interface{}, 100)
	out := make(chan interface{}, 100)

	wg := &sync.WaitGroup{}
	for _, job := range jobs {
		wg.Add(1)

		 func(in, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			job(in, out)
		 } (in, out, wg)

		fmt.Println("next step")
		wg.Wait()
		in = out;
		close(in)
		out = make(chan interface{}, 100)
	}
	close(out)

}

func SingleHash(in, out chan interface{}) {
	fmt.Println("SingleHash")

	for val := range in {
		data := fmt.Sprintf("%v", val)
		fmt.Println(data, " SingleHash data ", data)

		md5 := DataSignerMd5(data)
		fmt.Println(data, " SingleHash md5(data) ", md5)
		crc32md5 := DataSignerCrc32(md5)
		fmt.Println(data, " SingleHash crc32(md5(data)) ", crc32md5)
		crc32 := DataSignerCrc32(data)
		fmt.Println(data, " SingleHash crc32(data) ", crc32)

		fmt.Println(data, " SingleHash result ", crc32 + "~" + crc32md5)

		out <- crc32 + "~" + crc32md5
	}
}

func MultiHash(in, out chan interface{})  {
	fmt.Println("MultiHash")

	for val := range in {
		data := fmt.Sprintf("%v", val)

		hash := ""
		for i := 0; i < 6; i++ {
			crc32 := DataSignerCrc32(fmt.Sprintf("%v", i) + data)
			fmt.Println(data, " MultiHash crc32(th+step1) ", i, crc32)
			hash += crc32
		}

		out <- hash
	}
}

func CombineResults(in, out chan interface{})  {
	result := []string{}

	for val := range in {
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


	ExecutePipeline(hashSignJobs...)
}