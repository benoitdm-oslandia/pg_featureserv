package db_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/conf"
	"github.com/CrunchyData/pg_featureserv/internal/util"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

var cache = make(map[string]interface{})

func TestListen(t *testing.T) {
	db = util.CreateTestDb()
	defer util.CloseTestDb(db)

	dbURL := os.Getenv(conf.AppConfig.EnvDBURL)
	if dbURL == "" {
		dbURL = "postgresql://postgres@localhost:5433/pg_featureserv"
		log.Warnf("No env var '%s' defined, using default value: %s", conf.AppConfig.EnvDBURL, dbURL)
	}
	conf.Configuration.Database.DbConnection = dbURL
	conf.Configuration.Database.DbPoolMaxConnLifeTime = "1h"

	ctx := context.Background()
	dbconfig, errConf := pgxpool.ParseConfig(conf.Configuration.Database.DbConnection)
	if errConf != nil {
		log.Fatal(errConf)
	}
	db, errConn := pgxpool.ConnectConfig(ctx, dbconfig)
	if errConn != nil {
		log.Fatal(errConn)
	}

	conn, err := db.Acquire(ctx)
	if err != nil {
		if !pgconn.Timeout(err) {
			log.Fatal(err)
		}
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN table_update")
	if err != nil {
		if !pgconn.Timeout(err) {
			log.Fatal(err)
		}
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if !pgconn.Timeout(err) {
				log.Fatal(err)
			}
		}
		var notificationInterface map[string]interface{}

		errUnMarsh := json.Unmarshal([]byte(notification.Payload), &notificationInterface)
		if errUnMarsh != nil {
			log.Fatal(errUnMarsh)
		}

		delete(cache, notificationInterface["old_xmin"].(string))
		if notificationInterface["action"] != "DELETE" {
			cache[notificationInterface["new_xmin"].(string)] = notificationInterface["data"]
		}
		log.Printf("%v", cache)
	}
}
