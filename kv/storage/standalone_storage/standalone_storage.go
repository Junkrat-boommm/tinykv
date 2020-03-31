package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// interact with badger through `engine_util` API
	db := engine_util.CreateDB("sa_storage", conf)
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := BadgerReader{txn:txn}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.db.Update(func(txn *badger.Txn) error {
		//return txn.Set(KeyWithCF(cf, key), val)
		for _, value := range batch {
			switch op := value.Data.(type) {
			case storage.Put:
				return txn.Set(engine_util.KeyWithCF(op.Cf, op.Key), op.Value)
			case storage.Delete:
				return txn.Delete(engine_util.KeyWithCF(op.Cf, op.Key))
			default:
				return errors.New("No such modify type")
			}
		}
		return nil
	})
	//return nil
}


/* I need to return the StorageReader interface,
 * so I need to define a structure that implements all the methods in the interface
 */

type BadgerReader struct {
	txn *badger.Txn
}

func (reader BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	// val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	return engine_util.GetCFFromTxn(reader.txn, cf, key)
}
func (reader BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}
func (reader BadgerReader) Close() {
	reader.txn.Discard() //discard the transaction
}