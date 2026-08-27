package pooler

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func detailf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// toFrontend converts one ExecuteRequest payload into a pgproto3 message.
// Cancel is handled by the caller and returns nil.
func toFrontend(req *pgshardv1.ExecuteRequest) (pgproto3.FrontendMessage, error) {
	switch m := req.Message.(type) {
	case *pgshardv1.ExecuteRequest_SimpleQuery:
		return &pgproto3.Query{String: m.SimpleQuery.Sql}, nil
	case *pgshardv1.ExecuteRequest_Parse:
		return &pgproto3.Parse{Name: m.Parse.Name, Query: m.Parse.Sql, ParameterOIDs: m.Parse.ParamOids}, nil
	case *pgshardv1.ExecuteRequest_Bind:
		b := &pgproto3.Bind{DestinationPortal: m.Bind.Portal, PreparedStatement: m.Bind.Statement,
			ParameterFormatCodes: toInt16s(m.Bind.ParamFormats), ResultFormatCodes: toInt16s(m.Bind.ResultFormats)}
		b.Parameters = make([][]byte, len(m.Bind.Params))
		for i, v := range m.Bind.Params {
			if v.Null {
				continue
			}
			if v.Data == nil {
				b.Parameters[i] = []byte{}
			} else {
				b.Parameters[i] = v.Data
			}
		}
		return b, nil
	case *pgshardv1.ExecuteRequest_Execute:
		return &pgproto3.Execute{Portal: m.Execute.Portal, MaxRows: uint32(max(m.Execute.MaxRows, 0))}, nil
	case *pgshardv1.ExecuteRequest_Describe:
		return &pgproto3.Describe{ObjectType: describeKind(m.Describe.Kind), Name: m.Describe.Name}, nil
	case *pgshardv1.ExecuteRequest_Close:
		return &pgproto3.Close{ObjectType: closeKind(m.Close.Kind), Name: m.Close.Name}, nil
	case *pgshardv1.ExecuteRequest_Sync:
		return &pgproto3.Sync{}, nil
	case *pgshardv1.ExecuteRequest_CopyData:
		return &pgproto3.CopyData{Data: m.CopyData.Data}, nil
	case *pgshardv1.ExecuteRequest_CopyDone:
		return &pgproto3.CopyDone{}, nil
	case *pgshardv1.ExecuteRequest_CopyFail:
		return &pgproto3.CopyFail{Message: m.CopyFail.Message}, nil
	case *pgshardv1.ExecuteRequest_Cancel:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported request message %T", req.Message)
	}
}

func describeKind(k pgshardv1.Describe_Kind) byte {
	if k == pgshardv1.Describe_KIND_PORTAL {
		return 'P'
	}
	return 'S'
}

func closeKind(k pgshardv1.Close_Kind) byte {
	if k == pgshardv1.Close_KIND_PORTAL {
		return 'P'
	}
	return 'S'
}

func toInt16s(xs []int32) []int16 {
	if len(xs) == 0 {
		return nil
	}
	out := make([]int16, len(xs))
	for i, x := range xs {
		out[i] = int16(x)
	}
	return out
}

func toInt32s(xs []uint16) []int32 {
	out := make([]int32, len(xs))
	for i, x := range xs {
		out[i] = int32(x)
	}
	return out
}

// flushesBackend reports whether the message ends a batch the backend
// answers: the pooler flushes and pumps responses after it.
func flushesBackend(req *pgshardv1.ExecuteRequest) bool {
	switch req.Message.(type) {
	case *pgshardv1.ExecuteRequest_SimpleQuery, *pgshardv1.ExecuteRequest_Sync,
		*pgshardv1.ExecuteRequest_CopyDone, *pgshardv1.ExecuteRequest_CopyFail:
		return true
	}
	return false
}

// toResponse converts a backend message into an ExecuteResponse payload; nil
// means the message is not forwarded.
func toResponse(msg pgproto3.BackendMessage) *pgshardv1.ExecuteResponse {
	r := &pgshardv1.ExecuteResponse{}
	switch m := msg.(type) {
	case *pgproto3.RowDescription:
		fields := make([]*pgshardv1.FieldDescription, len(m.Fields))
		for i, f := range m.Fields {
			fields[i] = &pgshardv1.FieldDescription{Name: string(f.Name), TableOid: f.TableOID,
				ColumnAttr: int32(f.TableAttributeNumber), TypeOid: f.DataTypeOID, TypeSize: int32(f.DataTypeSize),
				TypeModifier: f.TypeModifier, Format: int32(f.Format)}
		}
		r.Message = &pgshardv1.ExecuteResponse_RowDescription{RowDescription: &pgshardv1.RowDescription{Fields: fields}}
	case *pgproto3.DataRow:
		cols := make([]*pgshardv1.Value, len(m.Values))
		for i, v := range m.Values {
			if v == nil {
				cols[i] = &pgshardv1.Value{Null: true}
			} else {
				cols[i] = &pgshardv1.Value{Data: v}
			}
		}
		r.Message = &pgshardv1.ExecuteResponse_DataRow{DataRow: &pgshardv1.DataRow{Columns: cols}}
	case *pgproto3.CommandComplete:
		r.Message = &pgshardv1.ExecuteResponse_CommandComplete{CommandComplete: &pgshardv1.CommandComplete{Tag: string(m.CommandTag)}}
	case *pgproto3.ErrorResponse:
		r.Message = &pgshardv1.ExecuteResponse_Error{Error: &pgshardv1.ErrorResponse{Error: &pgshardv1.Error{
			Sqlstate: m.Code, Message: m.Message, Detail: m.Detail, Hint: m.Hint}}}
	case *pgproto3.NoticeResponse:
		r.Message = &pgshardv1.ExecuteResponse_Notice{Notice: &pgshardv1.NoticeResponse{Notice: &pgshardv1.Error{
			Sqlstate: m.Code, Message: m.Message, Detail: m.Detail, Hint: m.Hint}}}
	case *pgproto3.NotificationResponse:
		r.Message = &pgshardv1.ExecuteResponse_Notification{Notification: &pgshardv1.NotificationResponse{
			Pid: m.PID, Channel: m.Channel, Payload: m.Payload}}
	case *pgproto3.ReadyForQuery:
		r.Message = &pgshardv1.ExecuteResponse_ReadyForQuery{ReadyForQuery: &pgshardv1.ReadyForQuery{TxnStatus: txnStatus(m.TxStatus)}}
	case *pgproto3.ParseComplete:
		r.Message = &pgshardv1.ExecuteResponse_ParseComplete{ParseComplete: &pgshardv1.ParseComplete{}}
	case *pgproto3.BindComplete:
		r.Message = &pgshardv1.ExecuteResponse_BindComplete{BindComplete: &pgshardv1.BindComplete{}}
	case *pgproto3.NoData:
		r.Message = &pgshardv1.ExecuteResponse_NoData{NoData: &pgshardv1.NoData{}}
	case *pgproto3.ParameterDescription:
		r.Message = &pgshardv1.ExecuteResponse_ParameterDescription{ParameterDescription: &pgshardv1.ParameterDescription{ParamOids: m.ParameterOIDs}}
	case *pgproto3.CopyInResponse:
		r.Message = &pgshardv1.ExecuteResponse_CopyInResponse{CopyInResponse: &pgshardv1.CopyInResponse{
			Format: int32(m.OverallFormat), ColumnFormats: toInt32s(m.ColumnFormatCodes)}}
	case *pgproto3.CopyOutResponse:
		r.Message = &pgshardv1.ExecuteResponse_CopyOutResponse{CopyOutResponse: &pgshardv1.CopyOutResponse{
			Format: int32(m.OverallFormat), ColumnFormats: toInt32s(m.ColumnFormatCodes)}}
	case *pgproto3.CopyData:
		r.Message = &pgshardv1.ExecuteResponse_CopyData{CopyData: &pgshardv1.CopyData{Data: m.Data}}
	case *pgproto3.CopyDone:
		r.Message = &pgshardv1.ExecuteResponse_CopyDone{CopyDone: &pgshardv1.CopyDone{}}
	case *pgproto3.ParameterStatus:
		r.Message = &pgshardv1.ExecuteResponse_ParameterStatus{ParameterStatus: &pgshardv1.ParameterStatus{Name: m.Name, Value: m.Value}}
	case *pgproto3.EmptyQueryResponse:
		r.Message = &pgshardv1.ExecuteResponse_EmptyQuery{EmptyQuery: &pgshardv1.EmptyQuery{}}
	case *pgproto3.PortalSuspended:
		r.Message = &pgshardv1.ExecuteResponse_PortalSuspended{PortalSuspended: &pgshardv1.PortalSuspended{}}
	default:
		return nil
	}
	return r
}

func txnStatus(b byte) pgshardv1.ReadyForQuery_TxnStatus {
	switch b {
	case 'I':
		return pgshardv1.ReadyForQuery_TXN_STATUS_IDLE
	case 'T':
		return pgshardv1.ReadyForQuery_TXN_STATUS_IN_TRANSACTION
	case 'E':
		return pgshardv1.ReadyForQuery_TXN_STATUS_FAILED
	}
	return pgshardv1.ReadyForQuery_TXN_STATUS_UNSPECIFIED
}

func errorResponse(sessionID string, e *pgshardv1.Error) *pgshardv1.ExecuteResponse {
	return &pgshardv1.ExecuteResponse{SessionId: sessionID, Message: &pgshardv1.ExecuteResponse_Error{Error: &pgshardv1.ErrorResponse{Error: e}}}
}
