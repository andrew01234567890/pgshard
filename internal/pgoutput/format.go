package pgoutput

import (
	"fmt"
	"strings"
)

// Format renders a message as one line of stable text, used by golden
// tests and debug logging. Tuple data is shown in text format.
func Format(m Message) string {
	switch v := m.(type) {
	case *Begin:
		return fmt.Sprintf("Begin xid=%d final=%X ts=%s", v.Xid, v.FinalLSN, v.CommitTime.UTC().Format("2006-01-02T15:04:05.000000Z"))
	case *Commit:
		return fmt.Sprintf("Commit flags=%d lsn=%X end=%X", v.Flags, v.CommitLSN, v.EndLSN)
	case *Origin:
		return fmt.Sprintf("Origin lsn=%X name=%s", v.CommitLSN, v.Name)
	case *Relation:
		cols := make([]string, len(v.Columns))
		for i, c := range v.Columns {
			key := ""
			if c.Key {
				key = "*"
			}
			cols[i] = fmt.Sprintf("%s%s:%d(%d)", key, c.Name, c.TypeOID, c.TypeMod)
		}
		return fmt.Sprintf("Relation%s id=%d %s.%s identity=%c cols=[%s]", xidTag(v.Xid), v.ID, v.Namespace, v.Name, v.ReplicaIdentity, strings.Join(cols, " "))
	case *Type:
		return fmt.Sprintf("Type%s id=%d %s.%s", xidTag(v.Xid), v.ID, v.Namespace, v.Name)
	case *Insert:
		return fmt.Sprintf("Insert%s rel=%d new=%s", xidTag(v.Xid), v.RelationID, formatTuple(&v.New))
	case *Update:
		return fmt.Sprintf("Update%s rel=%d key=%s old=%s new=%s", xidTag(v.Xid), v.RelationID, formatTuple(v.Key), formatTuple(v.Old), formatTuple(&v.New))
	case *Delete:
		return fmt.Sprintf("Delete%s rel=%d key=%s old=%s", xidTag(v.Xid), v.RelationID, formatTuple(v.Key), formatTuple(v.Old))
	case *Truncate:
		return fmt.Sprintf("Truncate%s rels=%v cascade=%t restart=%t", xidTag(v.Xid), v.RelationIDs, v.Cascade, v.RestartIdentity)
	case *LogicalMessage:
		return fmt.Sprintf("Message%s transactional=%t lsn=%X prefix=%s content=%q", xidTag(v.Xid), v.Transactional, v.LSN, v.Prefix, v.Content)
	case *StreamStart:
		return fmt.Sprintf("StreamStart xid=%d first=%t", v.Xid, v.FirstSegment)
	case *StreamStop:
		return "StreamStop"
	case *StreamCommit:
		return fmt.Sprintf("StreamCommit xid=%d flags=%d lsn=%X end=%X", v.Xid, v.Flags, v.CommitLSN, v.EndLSN)
	case *StreamAbort:
		return fmt.Sprintf("StreamAbort xid=%d subxid=%d lsn=%X", v.Xid, v.SubXid, v.AbortLSN)
	case *BeginPrepare:
		return fmt.Sprintf("BeginPrepare xid=%d gid=%s lsn=%X end=%X", v.Xid, v.Gid, v.PrepareLSN, v.EndLSN)
	case *Prepare:
		return fmt.Sprintf("Prepare xid=%d gid=%s flags=%d lsn=%X end=%X", v.Xid, v.Gid, v.Flags, v.PrepareLSN, v.EndLSN)
	case *CommitPrepared:
		return fmt.Sprintf("CommitPrepared xid=%d gid=%s flags=%d lsn=%X end=%X", v.Xid, v.Gid, v.Flags, v.CommitLSN, v.EndLSN)
	case *RollbackPrepared:
		return fmt.Sprintf("RollbackPrepared xid=%d gid=%s flags=%d prepare_end=%X rollback_end=%X", v.Xid, v.Gid, v.Flags, v.PrepareEndLSN, v.RollbackEndLSN)
	case *StreamPrepare:
		return fmt.Sprintf("StreamPrepare xid=%d gid=%s flags=%d lsn=%X end=%X", v.Xid, v.Gid, v.Flags, v.PrepareLSN, v.EndLSN)
	default:
		return fmt.Sprintf("%T", m)
	}
}

func xidTag(xid uint32) string {
	if xid == 0 {
		return ""
	}
	return fmt.Sprintf("[xid=%d]", xid)
}

func formatTuple(t *Tuple) string {
	if t == nil {
		return "-"
	}
	parts := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		switch c.Kind {
		case ColumnNull:
			parts[i] = "NULL"
		case ColumnUnchanged:
			parts[i] = "<unchanged>"
		case ColumnBinary:
			parts[i] = fmt.Sprintf("b:%x", c.Data)
		default:
			s := string(c.Data)
			if len(s) > 40 {
				s = fmt.Sprintf("%s...(%d bytes)", s[:20], len(s))
			}
			parts[i] = fmt.Sprintf("%q", s)
		}
	}
	return "(" + strings.Join(parts, ",") + ")"
}
