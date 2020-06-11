package dbs

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"time"
)

//Because it is used in many places, it encapsulates a method.
//The method to be implemented in this method is Exec's operator.

type DBTransaction struct {
	Commit func(mongo.SessionContext) error
	Run    func(mongo.SessionContext, func(mongo.SessionContext, DBTransaction) error) error
}

func NewDBTransaction() *DBTransaction {
	var dbTransaction = &DBTransaction{}
	dbTransaction.SetRun()
	dbTransaction.SetCommit()
	return dbTransaction
}

func (t *DBTransaction) SetCommit() {
	t.Commit = func(sctx mongo.SessionContext) error {
		err := sctx.CommitTransaction(sctx)

		if err != nil {
			return err
		}

		return nil
	}
}

func (t *DBTransaction) SetRun() {
	t.Run = func(sctx mongo.SessionContext, txnFn func(mongo.SessionContext, DBTransaction) error) error {
		err := txnFn(sctx, *t) // Performs transaction.
		if err == nil {
			return nil
		}
		zap.String("error", err.Error())

		return err
	}
}

func (t *DBTransaction) Exec(mongoClient *mongo.Client, operator func(mongo.SessionContext, DBTransaction) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	return mongoClient.UseSessionWithOptions(
		ctx, options.Session().SetDefaultReadPreference(readpref.Primary()),
		func(sctx mongo.SessionContext) error {
			return t.Run(sctx, operator)
		},
	)
}

//Specific call
//func SyncBlockData(node models.DBNode) error {
//	dbTransaction := db_session_service.NewDBTransaction(Logger)
//
//	// Updates two collections in a transaction.
//	updateEmployeeInfo := func(sctx mongo.SessionContext, d db_session_service.DBTransaction) error {
//		err := sctx.StartTransaction(options.Transaction().
//			SetReadConcern(readconcern.Snapshot()).
//			SetWriteConcern(writeconcern.New(writeconcern.WMajority())),
//		)
//		if err != nil {
//			return err
//		}
//		err = models.InsertNodeWithSession(sctx, node)
//		if err != nil {
//			_ = sctx.AbortTransaction(sctx)
//			return err
//		}
//
//		return d.Commit(sctx)
//	}
//
//	return dbTransaction.Exec(models.DB.Mongo, updateEmployeeInfo)
//}
