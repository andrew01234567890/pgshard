package router

import (
	"github.com/jackc/pgx/v5/pgproto3"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

func simpleQuery(sql string) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_SimpleQuery{SimpleQuery: &pgshardv1.SimpleQuery{Sql: sql}}}
}

func parseReq(name, sql string, oids []uint32) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Name: name, Sql: sql, ParamOids: oids}}}
}

func bindReq(portal, statement string, paramFormats []int16, params [][]byte, resultFormats []int16) *pgshardv1.ExecuteRequest {
	b := &pgshardv1.Bind{Portal: portal, Statement: statement, ParamFormats: toInt32s(paramFormats), ResultFormats: toInt32s(resultFormats)}
	b.Params = make([]*pgshardv1.Value, len(params))
	for i, p := range params {
		if p == nil {
			b.Params[i] = &pgshardv1.Value{Null: true}
		} else {
			b.Params[i] = &pgshardv1.Value{Data: p}
		}
	}
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Bind{Bind: b}}
}

func describeReq(kind pgwire.DescribeKind, name string) *pgshardv1.ExecuteRequest {
	k := pgshardv1.Describe_KIND_STATEMENT
	if kind == pgwire.DescribePortal {
		k = pgshardv1.Describe_KIND_PORTAL
	}
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Describe{Describe: &pgshardv1.Describe{Kind: k, Name: name}}}
}

func executeReq(portal string, maxRows int32) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Execute{Execute: &pgshardv1.ExecutePortal{Portal: portal, MaxRows: maxRows}}}
}

func closeReq(kind pgwire.DescribeKind, name string) *pgshardv1.ExecuteRequest {
	k := pgshardv1.Close_KIND_STATEMENT
	if kind == pgwire.DescribePortal {
		k = pgshardv1.Close_KIND_PORTAL
	}
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Close{Close: &pgshardv1.Close{Kind: k, Name: name}}}
}

func syncReq() *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Sync{Sync: &pgshardv1.Sync{}}}
}

func flushReq() *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Flush{Flush: &pgshardv1.Flush{}}}
}

func copyDataReq(data []byte) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_CopyData{CopyData: &pgshardv1.CopyData{Data: data}}}
}

func copyDoneReq() *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_CopyDone{CopyDone: &pgshardv1.CopyDone{}}}
}

func copyFailReq(msg string) *pgshardv1.ExecuteRequest {
	return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_CopyFail{CopyFail: &pgshardv1.CopyFail{Message: msg}}}
}

func toInt32s(xs []int16) []int32 {
	if len(xs) == 0 {
		return nil
	}
	out := make([]int32, len(xs))
	for i, x := range xs {
		out[i] = int32(x)
	}
	return out
}

func toUint16s(xs []int32) []uint16 {
	out := make([]uint16, len(xs))
	for i, x := range xs {
		out[i] = uint16(x)
	}
	return out
}

func fieldDescriptions(fs []*pgshardv1.FieldDescription) []pgproto3.FieldDescription {
	out := make([]pgproto3.FieldDescription, len(fs))
	for i, f := range fs {
		out[i] = pgproto3.FieldDescription{Name: []byte(f.Name), TableOID: f.TableOid, TableAttributeNumber: uint16(f.ColumnAttr),
			DataTypeOID: f.TypeOid, DataTypeSize: int16(f.TypeSize), TypeModifier: f.TypeModifier, Format: int16(f.Format)}
	}
	return out
}

func rowValues(cols []*pgshardv1.Value) [][]byte {
	out := make([][]byte, len(cols))
	for i, c := range cols {
		if c.Null {
			continue
		}
		if c.Data == nil {
			out[i] = []byte{}
		} else {
			out[i] = c.Data
		}
	}
	return out
}

func toPgwireError(e *pgshardv1.Error) *pgwire.Error {
	if e == nil {
		return pgwire.Errorf(pgwire.CodeInternalError, "pooler reported an error without details")
	}
	return &pgwire.Error{Severity: "ERROR", Code: e.Sqlstate, Message: e.Message, Detail: e.Detail, Hint: e.Hint}
}

func toNotice(e *pgshardv1.Error) *pgproto3.NoticeResponse {
	if e == nil {
		return &pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE"}
	}
	return &pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Code: e.Sqlstate, Message: e.Message, Detail: e.Detail, Hint: e.Hint}
}

func txStatus(s pgshardv1.ReadyForQuery_TxnStatus) pgwire.TxStatus {
	switch s {
	case pgshardv1.ReadyForQuery_TXN_STATUS_IN_TRANSACTION:
		return pgwire.TxInBlock
	case pgshardv1.ReadyForQuery_TXN_STATUS_FAILED:
		return pgwire.TxFailed
	default:
		return pgwire.TxIdle
	}
}
