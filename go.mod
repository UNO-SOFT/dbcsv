module github.com/UNO-SOFT/dbcsv

go 1.27

require (
	github.com/UNO-SOFT/spreadsheet v0.1.9
	github.com/UNO-SOFT/zlog v0.8.6
	github.com/extrame/xls v0.0.2-0.20180905092746-539786826ced
	github.com/google/go-cmp v0.7.0
	github.com/google/renameio/v2 v2.0.2
	github.com/klauspost/compress v1.19.0
	github.com/oracle/go-oracledb/v26 v26.0.0-beta
	github.com/peterbourgon/ff/v4 v4.0.0-beta.1
	github.com/xuri/excelize/v2 v2.11.0
	golang.org/x/sync v0.22.0
	golang.org/x/text v0.40.0
)

require (
	github.com/extrame/goyymmdd v0.0.0-20210114090516-7cc815f00d1a // indirect
	github.com/extrame/ole2 v0.0.0-20160812065207-d69429661ad7 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/godror/odbwrap v0.0.0-20260902104955-66a8bf53c021 // indirect
	github.com/richardlehane/mscfb v1.0.7 // indirect
	github.com/richardlehane/msoleps v1.0.6 // indirect
	github.com/tiendc/go-deepcopy v1.7.2 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/quicktemplate v1.8.0 // indirect
	github.com/xuri/efp v0.0.1 // indirect
	github.com/xuri/nfp v0.0.2-0.20250530014748-2ddeb826f9a9 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/exp v0.0.0-20260611194520-c48552f49976 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/term v0.45.0 // indirect
)

// replace github.com/godror/godror => ../../godror/godror
//replace github.com/UNO-SOFT/spreadsheet => ../../UNO-SOFT/spreadsheet

replace github.com/peterbourgon/ff/v4 v4.0.0-beta.1 => github.com/UNO-SOFT/ff/v4 v4.0.0-beta.1.us

replace github.com/oracle/go-oracledb/v26 v26.0.0-beta => github.com/UNO-SOFT/go-oracledb/v26 v26.0.0-20260830074709-76c26a4bf362
