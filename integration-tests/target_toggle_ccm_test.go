package integration_tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/httpzdmproxy"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
)

const toggleTable = "toggle_test_data"
const toggleCounterTable = "toggle_test_counters"
const toggleBatchA = "toggle_test_batch_a"
const toggleBatchB = "toggle_test_batch_b"
const togglePreparedTable = "toggle_test_prepared"
const toggleLoadTable = "toggle_test_load"

const toggleHTTPAddr = "localhost:14098"

// startToggleHTTPServer creates a dedicated HTTP server for metrics and target toggle API.
func startToggleHTTPServer(t *testing.T, proxyInstance *zdmproxy.ZdmProxy) *http.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle("/metrics", proxyInstance.GetMetricHandler().GetHttpHandler())
	mux.Handle("/api/v1/target", httpzdmproxy.TargetHandler(proxyInstance))
	mux.Handle("/api/v1/target/enable", httpzdmproxy.TargetHandler(proxyInstance))
	mux.Handle("/api/v1/target/disable", httpzdmproxy.TargetHandler(proxyInstance))
	srv := &http.Server{Addr: toggleHTTPAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Warnf("toggle test http server error: %v", err)
		}
	}()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/target", toggleHTTPAddr))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "HTTP server did not start")

	return srv
}

// createToggleTables creates the test tables on a given session.
func createToggleTables(t *testing.T, s *gocql.Session) {
	t.Helper()
	tables := []string{toggleTable, toggleBatchA, toggleBatchB, togglePreparedTable, toggleLoadTable}
	for _, tbl := range tables {
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, tbl)).Exec()
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, name text)", setup.TestKeyspace, tbl)).Exec())
	}
	s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, toggleCounterTable)).Exec()
	require.Nil(t, s.Query(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, count counter)", setup.TestKeyspace, toggleCounterTable)).Exec())
}

// TestTargetToggleCCM tests the full lifecycle of disabling and re-enabling the target cluster
// at runtime via the REST API.
//
// Phases:
//  1. Write broad set (inline, prepared, batch, counter) with target enabled -> verify metrics on both clusters.
//  2. Disable target via API -> write same broad set -> verify only origin metrics increment.
//  3. While disabled, create NEW prepared statements (these only go to origin).
//  4. Re-enable target via API -> use those new prepared statements -> verify they work
//     (UNPREPARED recovery re-prepares them on target) -> verify both metrics resume.
//  5. Verify reads work throughout (SELECT on origin while target disabled).
//  6. Multiple rapid toggle cycles to check stability.
func TestTargetToggleCCM(t *testing.T) {
	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	srv := startToggleHTTPServer(t, proxyInstance)
	defer srv.Close()

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	createToggleTables(t, originCluster.GetSession())
	createToggleTables(t, targetCluster.GetSession())

	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	ks := setup.TestKeyspace

	// ================================================================
	// PHASE 1: Target enabled — writes go to both clusters
	// ================================================================
	t.Run("phase1_target_enabled", func(t *testing.T) {
		requireTargetStatus(t, true)

		// Inline INSERT
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000001, 'p1_inline')", ks, toggleTable)).Exec())

		// Inline UPDATE
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET name = 'p1_inline_updated' WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleTable)).Exec())

		// Inline DELETE + re-insert
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"DELETE FROM %s.%s WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleTable)).Exec())
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000001, 'p1_inline')", ks, toggleTable)).Exec())

		// Inline counter
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleCounterTable)).Exec())

		// Prepared INSERT
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleTable))
		q.Bind("a0000001-0000-0000-0000-000000000002", "p1_prepared")
		require.Nil(t, q.Exec())

		// Prepared UPDATE
		q2 := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET name = ? WHERE id = ?", ks, toggleTable))
		q2.Bind("p1_prepared_updated", "a0000001-0000-0000-0000-000000000002")
		require.Nil(t, q2.Exec())

		// Prepared DELETE
		q3 := proxy.Query(fmt.Sprintf("DELETE FROM %s.%s WHERE id = ?", ks, toggleTable))
		q3.Bind("a0000001-0000-0000-0000-000000000002")
		require.Nil(t, q3.Exec())

		// Re-insert for later verification
		q4 := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleTable))
		q4.Bind("a0000001-0000-0000-0000-000000000002", "p1_prepared")
		require.Nil(t, q4.Exec())

		// Prepared counter
		qc := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + ? WHERE id = ?", ks, toggleCounterTable))
		qc.Bind(int64(5), "a0000001-0000-0000-0000-000000000002")
		require.Nil(t, qc.Exec())

		// Batch with inline children
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000003, 'p1_batch_a')", ks, toggleBatchA))
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000003, 'p1_batch_b')", ks, toggleBatchB))
		require.Nil(t, proxy.ExecuteBatch(batch))

		// Batch with prepared children
		batch2 := proxy.NewBatch(gocql.LoggedBatch)
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchA), "a0000001-0000-0000-0000-000000000004", "p1_batch_prep_a")
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchB), "a0000001-0000-0000-0000-000000000004", "p1_batch_prep_b")
		require.Nil(t, proxy.ExecuteBatch(batch2))

		// Batch with mixed inline and prepared children
		batch3 := proxy.NewBatch(gocql.LoggedBatch)
		batch3.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000005, 'p1_mixed_inline')", ks, toggleBatchA))
		batch3.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchB), "a0000001-0000-0000-0000-000000000005", "p1_mixed_prepared")
		require.Nil(t, proxy.ExecuteBatch(batch3))

		// Counter batch
		counterBatch := proxy.NewBatch(gocql.CounterBatch)
		counterBatch.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + 1 WHERE id = a0000001-0000-0000-0000-000000000003", ks, toggleCounterTable))
		require.Nil(t, proxy.ExecuteBatch(counterBatch))

		// Verify metrics on both clusters
		lines := gatherToggleMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", ks, toggleTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", ks, toggleTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", ks, toggleBatchA)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", ks, toggleBatchA)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", ks, toggleBatchB)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", ks, toggleBatchB)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", ks, toggleCounterTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", ks, toggleCounterTable)

		// Verify data exists on target
		var name string
		err := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p1_inline", name)

		// Verify SELECT works through proxy
		err = proxy.Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p1_inline", name)
	})

	// Capture metric values before disabling
	originCountBefore := getMetricValue(t, ks, toggleTable, "origin")
	targetCountBefore := getMetricValue(t, ks, toggleTable, "target")
	require.True(t, originCountBefore > 0, "origin count should be > 0 before disable")
	require.True(t, targetCountBefore > 0, "target count should be > 0 before disable")

	// ================================================================
	// PHASE 2: Disable target — writes go to origin only
	// ================================================================
	t.Run("phase2_target_disabled", func(t *testing.T) {
		postTargetToggle(t, "disable")
		requireTargetStatus(t, false)

		// Inline INSERT
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (b0000001-0000-0000-0000-000000000001, 'p2_inline')", ks, toggleTable)).Exec())

		// Inline UPDATE
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET name = 'p2_inline_updated' WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleTable)).Exec())

		// Inline DELETE
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"DELETE FROM %s.%s WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleTable)).Exec())

		// Re-insert for verification
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (b0000001-0000-0000-0000-000000000001, 'p2_inline')", ks, toggleTable)).Exec())

		// Inline counter
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleCounterTable)).Exec())

		// Prepared INSERT
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleTable))
		q.Bind("b0000001-0000-0000-0000-000000000002", "p2_prepared")
		require.Nil(t, q.Exec())

		// Prepared UPDATE
		q2 := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET name = ? WHERE id = ?", ks, toggleTable))
		q2.Bind("p2_prepared_updated", "b0000001-0000-0000-0000-000000000002")
		require.Nil(t, q2.Exec())

		// Prepared DELETE
		q3 := proxy.Query(fmt.Sprintf("DELETE FROM %s.%s WHERE id = ?", ks, toggleTable))
		q3.Bind("b0000001-0000-0000-0000-000000000002")
		require.Nil(t, q3.Exec())

		// Batch with inline children
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (b0000001-0000-0000-0000-000000000003, 'p2_batch_a')", ks, toggleBatchA))
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (b0000001-0000-0000-0000-000000000003, 'p2_batch_b')", ks, toggleBatchB))
		require.Nil(t, proxy.ExecuteBatch(batch))

		// Batch with prepared children
		batch2 := proxy.NewBatch(gocql.LoggedBatch)
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchA), "b0000001-0000-0000-0000-000000000004", "p2_batch_prep_a")
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchB), "b0000001-0000-0000-0000-000000000004", "p2_batch_prep_b")
		require.Nil(t, proxy.ExecuteBatch(batch2))

		// Counter batch
		counterBatch := proxy.NewBatch(gocql.CounterBatch)
		counterBatch.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + 1 WHERE id = b0000001-0000-0000-0000-000000000003", ks, toggleCounterTable))
		require.Nil(t, proxy.ExecuteBatch(counterBatch))

		// Verify: origin metrics increased, target metrics did NOT
		originCountAfter := getMetricValue(t, ks, toggleTable, "origin")
		targetCountAfter := getMetricValue(t, ks, toggleTable, "target")
		require.True(t, originCountAfter > originCountBefore,
			"origin write count should have increased: before=%v after=%v", originCountBefore, originCountAfter)
		require.Equal(t, targetCountBefore, targetCountAfter,
			"target write count should NOT have changed: before=%v after=%v", targetCountBefore, targetCountAfter)

		// Verify data NOT on target
		iter := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleTable)).Iter()
		require.Equal(t, 0, iter.NumRows(), "data written during disabled period should NOT be on target")
		iter.Close()

		// Verify data IS on origin
		var name string
		err := originCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p2_inline", name)

		// Verify SELECT still works through proxy while target disabled
		err = proxy.Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p1_inline", name)

		// Verify gauge shows disabled
		lines := gatherToggleMetricLines(t)
		requireTargetEnabledGauge(t, lines, 0)
	})

	// ================================================================
	// PHASE 3: While disabled, create NEW prepared statements
	// ================================================================
	t.Run("phase3_new_prepared_while_disabled", func(t *testing.T) {
		requireTargetStatus(t, false)

		// New prepared INSERT on a table not previously used with prepared statements
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, togglePreparedTable))
		q.Bind("c0000001-0000-0000-0000-000000000001", "p3_new_prepared")
		require.Nil(t, q.Exec())

		// New prepared UPDATE
		q2 := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET name = ? WHERE id = ?", ks, togglePreparedTable))
		q2.Bind("p3_updated", "c0000001-0000-0000-0000-000000000001")
		require.Nil(t, q2.Exec())

		// New prepared DELETE
		q3 := proxy.Query(fmt.Sprintf("DELETE FROM %s.%s WHERE id = ?", ks, togglePreparedTable))
		q3.Bind("c0000001-0000-0000-0000-000000000001")
		require.Nil(t, q3.Exec())

		// Re-insert for later use in phase 4
		q4 := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, togglePreparedTable))
		q4.Bind("c0000001-0000-0000-0000-000000000001", "p3_final")
		require.Nil(t, q4.Exec())

		// Batch with new prepared children on togglePreparedTable
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, togglePreparedTable), "c0000001-0000-0000-0000-000000000002", "p3_batch_prep")
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchA), "c0000001-0000-0000-0000-000000000002", "p3_batch_a")
		require.Nil(t, proxy.ExecuteBatch(batch))

		// Verify data on origin only
		var name string
		err := originCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = c0000001-0000-0000-0000-000000000001", ks, togglePreparedTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p3_final", name)

		// Verify NOT on target
		iter := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = c0000001-0000-0000-0000-000000000001", ks, togglePreparedTable)).Iter()
		require.Equal(t, 0, iter.NumRows(), "data from phase 3 should NOT be on target")
		iter.Close()
	})

	// ================================================================
	// PHASE 4: Re-enable target — use the NEW prepared statements
	// ================================================================
	t.Run("phase4_reenable_target", func(t *testing.T) {
		postTargetToggle(t, "enable")
		requireTargetStatus(t, true)

		// Use prepared statements created during disabled period.
		// gocql receives UNPREPARED from target, auto-re-prepares, and retries.
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, togglePreparedTable))
		q.Bind("d0000001-0000-0000-0000-000000000001", "p4_after_reenable")
		require.Nil(t, q.Exec())

		// Use the prepared UPDATE from phase 3
		q2 := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET name = ? WHERE id = ?", ks, togglePreparedTable))
		q2.Bind("p4_updated_reenable", "d0000001-0000-0000-0000-000000000001")
		require.Nil(t, q2.Exec())

		// Inline insert
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (d0000001-0000-0000-0000-000000000002, 'p4_inline')", ks, toggleTable)).Exec())

		// Counter
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = d0000001-0000-0000-0000-000000000001", ks, toggleCounterTable)).Exec())

		// Batch with prepared children
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchA), "d0000001-0000-0000-0000-000000000003", "p4_batch_a")
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchB), "d0000001-0000-0000-0000-000000000003", "p4_batch_b")
		require.Nil(t, proxy.ExecuteBatch(batch))

		// Verify metrics: both should now be incrementing
		originCountFinal := getMetricValue(t, ks, toggleTable, "origin")
		targetCountFinal := getMetricValue(t, ks, toggleTable, "target")
		require.True(t, originCountFinal > originCountBefore,
			"origin count should be higher: initial=%v final=%v", originCountBefore, originCountFinal)
		require.True(t, targetCountFinal > targetCountBefore,
			"target count should be higher (writes resumed): initial=%v final=%v", targetCountBefore, targetCountFinal)

		// Verify data on TARGET for phase 4 writes
		var name string
		err := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = d0000001-0000-0000-0000-000000000001", ks, togglePreparedTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p4_updated_reenable", name, "prepared data should be on target after re-enable")

		err = targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = d0000001-0000-0000-0000-000000000002", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p4_inline", name, "inline write should be on target after re-enable")

		err = targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = d0000001-0000-0000-0000-000000000003", ks, toggleBatchA)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p4_batch_a", name, "batch write should be on target after re-enable")

		// Verify gauge shows enabled
		lines := gatherToggleMetricLines(t)
		requireTargetEnabledGauge(t, lines, 1)
	})

	// ================================================================
	// PHASE 5: Rapid toggle cycles — stability test
	// ================================================================
	t.Run("phase5_rapid_toggle_cycles", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			postTargetToggle(t, "disable")
			requireTargetStatus(t, false)

			// Write while disabled — should succeed
			require.Nil(t, proxy.Query(fmt.Sprintf(
				"INSERT INTO %s.%s (id, name) VALUES (e%07d-0000-0000-0000-000000000001, 'cycle_%d')",
				ks, toggleTable, i, i)).Exec())

			postTargetToggle(t, "enable")
			requireTargetStatus(t, true)

			// Write while enabled — should succeed on both
			require.Nil(t, proxy.Query(fmt.Sprintf(
				"INSERT INTO %s.%s (id, name) VALUES (e%07d-0000-0000-0000-000000000002, 'cycle_%d_enabled')",
				ks, toggleTable, i, i)).Exec())

			// Verify the enabled write reached target
			var name string
			err := targetCluster.GetSession().Query(fmt.Sprintf(
				"SELECT name FROM %s.%s WHERE id = e%07d-0000-0000-0000-000000000002", ks, toggleTable, i)).Scan(&name)
			require.Nil(t, err)
			require.Equal(t, fmt.Sprintf("cycle_%d_enabled", i), name)
		}
	})
}

// TestTargetToggleEdgeCasesCCM tests edge cases that could break under target toggle:
// schema changes, USE keyspace, reads while disabled, multiple new connections while
// disabled, mixed read/write workload, and DDL+DML lifecycle across toggle.
func TestTargetToggleEdgeCasesCCM(t *testing.T) {
	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	srv := startToggleHTTPServer(t, proxyInstance)
	defer srv.Close()

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	ks := setup.TestKeyspace

	// Create a shared table for edge case tests
	edgeTable := "toggle_edge_test"
	for _, s := range []*gocql.Session{originCluster.GetSession(), targetCluster.GetSession()} {
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, edgeTable)).Exec()
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, name text)", ks, edgeTable)).Exec())
	}

	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	// ================================================================
	// Test: SELECT reads work while target is disabled
	// ================================================================
	t.Run("reads_while_disabled", func(t *testing.T) {
		// Write some data while enabled
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (f0000001-0000-0000-0000-000000000001, 'read_test')", ks, edgeTable)).Exec())

		postTargetToggle(t, "disable")
		defer postTargetToggle(t, "enable")

		// SELECT should still work — reads go to origin
		var name string
		err := proxy.Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = f0000001-0000-0000-0000-000000000001", ks, edgeTable)).Scan(&name)
		require.Nil(t, err, "SELECT should work while target disabled")
		require.Equal(t, "read_test", name)

		// Multiple reads
		for i := 0; i < 20; i++ {
			err = proxy.Query(fmt.Sprintf(
				"SELECT name FROM %s.%s WHERE id = f0000001-0000-0000-0000-000000000001", ks, edgeTable)).Scan(&name)
			require.Nil(t, err, "repeated SELECT %d should work while target disabled", i)
		}
	})

	// ================================================================
	// Test: Writes with fully qualified keyspace.table while disabled
	// (gocql doesn't support USE statements directly)
	// ================================================================
	t.Run("qualified_writes_while_disabled", func(t *testing.T) {
		postTargetToggle(t, "disable")
		defer postTargetToggle(t, "enable")

		// Write with fully qualified keyspace.table — should succeed on origin only
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (f0000002-0000-0000-0000-000000000001, 'qualified')", ks, edgeTable)).Exec())

		// Verify data on origin
		var name string
		err = originCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = f0000002-0000-0000-0000-000000000001", ks, edgeTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "qualified", name)

		// Verify NOT on target
		iter := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = f0000002-0000-0000-0000-000000000001", ks, edgeTable)).Iter()
		require.Equal(t, 0, iter.NumRows(), "data should NOT be on target while disabled")
		iter.Close()
	})

	// ================================================================
	// Test: Schema changes (DDL) while target disabled
	// ================================================================
	t.Run("schema_changes_while_disabled", func(t *testing.T) {
		postTargetToggle(t, "disable")

		// CREATE TABLE through proxy while disabled — should work on origin only
		ddlTable := "toggle_ddl_test"
		proxy.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, ddlTable)).Exec()
		err := proxy.Query(fmt.Sprintf(
			"CREATE TABLE %s.%s (id uuid PRIMARY KEY, val text)", ks, ddlTable)).Exec()
		require.Nil(t, err, "CREATE TABLE should work while target disabled")

		// Write to the new table
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, val) VALUES (f0000003-0000-0000-0000-000000000001, 'ddl_test')", ks, ddlTable)).Exec())

		// Verify on origin
		var val string
		err = originCluster.GetSession().Query(fmt.Sprintf(
			"SELECT val FROM %s.%s WHERE id = f0000003-0000-0000-0000-000000000001", ks, ddlTable)).Scan(&val)
		require.Nil(t, err)
		require.Equal(t, "ddl_test", val)

		// Re-enable target
		postTargetToggle(t, "enable")

		// Create the table on target manually (it doesn't exist there yet)
		require.Nil(t, targetCluster.GetSession().Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, val text)", ks, ddlTable)).Exec())

		// Now writes should go to both
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, val) VALUES (f0000003-0000-0000-0000-000000000002, 'after_reenable')", ks, ddlTable)).Exec())

		// Verify on target
		err = targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT val FROM %s.%s WHERE id = f0000003-0000-0000-0000-000000000002", ks, ddlTable)).Scan(&val)
		require.Nil(t, err)
		require.Equal(t, "after_reenable", val)
	})

	// ================================================================
	// Test: Multiple new connections while target is disabled
	// ================================================================
	t.Run("multiple_new_connections_while_disabled", func(t *testing.T) {
		postTargetToggle(t, "disable")
		defer postTargetToggle(t, "enable")

		// Open several new sessions while disabled — each creates a new
		// ClientHandler without a target connector
		sessions := make([]*gocql.Session, 5)
		for i := 0; i < 5; i++ {
			s, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
			require.Nil(t, err, "new connection %d should succeed while target disabled", i)
			sessions[i] = s
		}

		// Write through each session
		for i, s := range sessions {
			err := s.Query(fmt.Sprintf(
				"INSERT INTO %s.%s (id, name) VALUES (f0000004-0000-0000-0000-%012d, 'conn_%d')",
				ks, edgeTable, i, i)).Exec()
			require.Nil(t, err, "write on connection %d should succeed while target disabled", i)
		}

		// Read through each session
		for i, s := range sessions {
			var name string
			err := s.Query(fmt.Sprintf(
				"SELECT name FROM %s.%s WHERE id = f0000004-0000-0000-0000-%012d",
				ks, edgeTable, i)).Scan(&name)
			require.Nil(t, err, "read on connection %d should work while target disabled", i)
			require.Equal(t, fmt.Sprintf("conn_%d", i), name)
		}

		// Close all sessions
		for _, s := range sessions {
			s.Close()
		}
	})

	// ================================================================
	// Test: Interleaved reads and writes during toggle
	// ================================================================
	t.Run("interleaved_reads_writes_during_toggle", func(t *testing.T) {
		// Seed data
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (f0000005-0000-0000-0000-000000000001, 'seed')", ks, edgeTable)).Exec())

		for cycle := 0; cycle < 3; cycle++ {
			// Disable
			postTargetToggle(t, "disable")

			// Mix of reads and writes
			var name string
			err := proxy.Query(fmt.Sprintf(
				"SELECT name FROM %s.%s WHERE id = f0000005-0000-0000-0000-000000000001", ks, edgeTable)).Scan(&name)
			require.Nil(t, err, "read in cycle %d should work while disabled", cycle)

			require.Nil(t, proxy.Query(fmt.Sprintf(
				"INSERT INTO %s.%s (id, name) VALUES (f0000005-0000-0000-0000-%012d, 'cycle_%d')",
				ks, edgeTable, cycle+10, cycle)).Exec())

			err = proxy.Query(fmt.Sprintf(
				"SELECT name FROM %s.%s WHERE id = f0000005-0000-0000-0000-%012d",
				ks, edgeTable, cycle+10)).Scan(&name)
			require.Nil(t, err, "read after write in cycle %d should work while disabled", cycle)
			require.Equal(t, fmt.Sprintf("cycle_%d", cycle), name)

			// Re-enable
			postTargetToggle(t, "enable")

			// Verify reads and writes work after re-enable
			require.Nil(t, proxy.Query(fmt.Sprintf(
				"INSERT INTO %s.%s (id, name) VALUES (f0000005-0000-0000-0000-%012d, 'enabled_%d')",
				ks, edgeTable, cycle+20, cycle)).Exec())

			err = proxy.Query(fmt.Sprintf(
				"SELECT name FROM %s.%s WHERE id = f0000005-0000-0000-0000-%012d",
				ks, edgeTable, cycle+20)).Scan(&name)
			require.Nil(t, err, "read after re-enable in cycle %d should work", cycle)
			require.Equal(t, fmt.Sprintf("enabled_%d", cycle), name)
		}
	})

	// ================================================================
	// Test: Prepared statements survive multiple toggle cycles
	// ================================================================
	t.Run("prepared_statements_survive_multiple_cycles", func(t *testing.T) {
		// Prepare a statement while enabled
		insertQ := fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, edgeTable)
		selectQ := fmt.Sprintf("SELECT name FROM %s.%s WHERE id = ?", ks, edgeTable)

		// First use while enabled — gocql auto-prepares
		q := proxy.Query(insertQ)
		q.Bind("f0000006-0000-0000-0000-000000000001", "ps_cycle_0")
		require.Nil(t, q.Exec())

		// Toggle 5 times, using the same prepared statements each time
		for cycle := 0; cycle < 5; cycle++ {
			postTargetToggle(t, "disable")

			// Use existing prepared statement while disabled
			q = proxy.Query(insertQ)
			q.Bind(fmt.Sprintf("f0000006-0000-0000-0000-%012d", cycle+10), fmt.Sprintf("disabled_%d", cycle))
			require.Nil(t, q.Exec(), "prepared INSERT should work in disabled cycle %d", cycle)

			// Read with prepared statement
			var name string
			q = proxy.Query(selectQ)
			q.Bind(fmt.Sprintf("f0000006-0000-0000-0000-%012d", cycle+10))
			require.Nil(t, q.Scan(&name), "prepared SELECT should work in disabled cycle %d", cycle)
			require.Equal(t, fmt.Sprintf("disabled_%d", cycle), name)

			postTargetToggle(t, "enable")

			// Use same prepared statement while enabled — should trigger
			// UNPREPARED recovery on target for first use after re-enable
			q = proxy.Query(insertQ)
			q.Bind(fmt.Sprintf("f0000006-0000-0000-0000-%012d", cycle+20), fmt.Sprintf("enabled_%d", cycle))
			require.Nil(t, q.Exec(), "prepared INSERT should work in enabled cycle %d", cycle)

			// Verify on target
			var targetName string
			err := targetCluster.GetSession().Query(fmt.Sprintf(
				"SELECT name FROM %s.%s WHERE id = f0000006-0000-0000-0000-%012d",
				ks, edgeTable, cycle+20)).Scan(&targetName)
			require.Nil(t, err, "data should be on target after re-enable in cycle %d", cycle)
			require.Equal(t, fmt.Sprintf("enabled_%d", cycle), targetName)
		}
	})

	// ================================================================
	// Test: system.local and system.peers queries while disabled
	// ================================================================
	t.Run("system_queries_while_disabled", func(t *testing.T) {
		postTargetToggle(t, "disable")
		defer postTargetToggle(t, "enable")

		// system.local query — these are intercepted by the proxy
		var clusterName string
		err := proxy.Query("SELECT cluster_name FROM system.local").Scan(&clusterName)
		require.Nil(t, err, "system.local query should work while target disabled")
		require.NotEmpty(t, clusterName)

		// system.peers query
		iter := proxy.Query("SELECT peer FROM system.peers").Iter()
		iter.Close()
		// No assertion on content — just verify it doesn't panic
	})
}

// TestTargetToggleConcurrentLoadCCM tests the target toggle under concurrent write load.
// Multiple goroutines continuously write to the proxy while the target is toggled on and off.
// Verifies no panics, no deadlocks, and writes succeed throughout.
func TestTargetToggleConcurrentLoadCCM(t *testing.T) {
	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	srv := startToggleHTTPServer(t, proxyInstance)
	defer srv.Close()

	originCluster, _, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	createToggleTables(t, originCluster.GetSession())
	// Target tables are shared from global clusters, already created by previous test or setup

	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	ks := setup.TestKeyspace

	const numWorkers = 10
	const writesPerWorker = 50
	var successCount int64
	var errorCount int64
	var wg sync.WaitGroup

	// Start concurrent writers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				id := fmt.Sprintf("f%07d-0000-0000-0000-%012d", workerID, i)
				err := proxy.Query(fmt.Sprintf(
					"INSERT INTO %s.%s (id, name) VALUES (%s, 'load_w%d_i%d')",
					ks, toggleLoadTable, id, workerID, i)).Exec()
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					log.Debugf("Worker %d write %d error: %v", workerID, i, err)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(w)
	}

	// Toggle target on and off during the writes
	time.Sleep(100 * time.Millisecond) // let writes start
	for cycle := 0; cycle < 3; cycle++ {
		postTargetToggle(t, "disable")
		time.Sleep(200 * time.Millisecond)
		postTargetToggle(t, "enable")
		time.Sleep(200 * time.Millisecond)
	}

	wg.Wait()

	total := atomic.LoadInt64(&successCount) + atomic.LoadInt64(&errorCount)
	t.Logf("Concurrent load test: %d/%d writes succeeded, %d errors",
		atomic.LoadInt64(&successCount), total, atomic.LoadInt64(&errorCount))

	// The vast majority of writes should succeed. A small number may fail during
	// the brief transition window when the toggle flips and in-flight requests to
	// target may not complete. Allow up to 5% error rate.
	maxAllowedErrors := int64(numWorkers * writesPerWorker * 5 / 100)
	require.True(t, atomic.LoadInt64(&errorCount) <= maxAllowedErrors,
		"error count %d exceeds allowed threshold %d", atomic.LoadInt64(&errorCount), maxAllowedErrors)
	require.True(t, atomic.LoadInt64(&successCount) > 0, "at least some writes should succeed")

	// Verify at least some data on origin
	var count int
	iter := originCluster.GetSession().Query(fmt.Sprintf(
		"SELECT id FROM %s.%s", ks, toggleLoadTable)).Iter()
	count = iter.NumRows()
	iter.Close()
	require.True(t, count > 0, "origin should have data from load test")
}

// TestTargetToggleOutageCCM simulates a real production scenario:
//  1. Start writing to both clusters (target enabled)
//  2. Stop the target CCM node (simulating outage)
//  3. Observe errors from the proxy (target is down but still enabled)
//  4. Disable target via API
//  5. Writes succeed again (origin only)
//  6. Restart the target CCM node
//  7. Re-enable target via API
//  8. New connections write to both clusters again
//  9. Verify data on target for writes after re-enable
func TestTargetToggleOutageCCM(t *testing.T) {
	// Use temporary clusters so stopping nodes doesn't affect other tests
	tempCcmSetup, err := setup.NewTemporaryCcmTestSetup(t, true, false)
	require.Nil(t, err)
	defer tempCcmSetup.Cleanup()

	// Create tables
	outageTable := "toggle_outage_test"
	for _, s := range []*gocql.Session{tempCcmSetup.Origin.GetSession(), tempCcmSetup.Target.GetSession()} {
		s.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", setup.TestKeyspace)).Exec()
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, outageTable)).Exec()
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, name text)", setup.TestKeyspace, outageTable)).Exec())
	}

	// Create proxy with config pointing to temp clusters
	testConfig := setup.NewTestConfig(tempCcmSetup.Origin.GetInitialContactPoint(), tempCcmSetup.Target.GetInitialContactPoint())
	testConfig.TargetEnableHostAssignment = false
	testConfig.OriginEnableHostAssignment = false
	proxyInstance, err := setup.NewProxyInstanceWithConfig(testConfig)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	// HTTP server for toggle API (use a different port to avoid conflicts)
	outageHTTPAddr := "localhost:14097"
	mux := http.NewServeMux()
	mux.Handle("/api/v1/target", httpzdmproxy.TargetHandler(proxyInstance))
	mux.Handle("/api/v1/target/enable", httpzdmproxy.TargetHandler(proxyInstance))
	mux.Handle("/api/v1/target/disable", httpzdmproxy.TargetHandler(proxyInstance))
	srv := &http.Server{Addr: outageHTTPAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Warnf("outage test http server error: %v", err)
		}
	}()
	defer srv.Close()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/target", outageHTTPAddr))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "HTTP server did not start")

	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	ks := setup.TestKeyspace

	// Step 1: Write with both clusters up
	t.Run("step1_both_up", func(t *testing.T) {
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (aa000001-0000-0000-0000-000000000001, 'both_up')", ks, outageTable)).Exec())

		// Verify on target
		var name string
		err := tempCcmSetup.Target.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = aa000001-0000-0000-0000-000000000001", ks, outageTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "both_up", name)
	})

	// Step 2: Stop the target node
	t.Run("step2_stop_target", func(t *testing.T) {
		err := tempCcmSetup.Target.StopNode(0)
		require.Nil(t, err, "failed to stop target node")
		t.Log("Target node stopped")
	})

	// Step 3: Writes should fail or timeout (target is down but still enabled)
	t.Run("step3_writes_fail_target_down", func(t *testing.T) {
		// Give the proxy a moment to detect the down node
		time.Sleep(2 * time.Second)

		// This write may fail/timeout because target is down
		err := proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (aa000001-0000-0000-0000-000000000002, 'should_fail')", ks, outageTable)).Exec()
		// We expect an error here — target is down
		t.Logf("Write with target down: err=%v (error expected)", err)
	})

	// Step 4: Disable target via API
	t.Run("step4_disable_target", func(t *testing.T) {
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/target/disable", outageHTTPAddr), "application/json", nil)
		require.Nil(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		t.Log("Target disabled via API")
	})

	// Step 5: Writes succeed now (origin only, target disabled)
	t.Run("step5_writes_succeed_target_disabled", func(t *testing.T) {
		// New connection since old one might be in a bad state
		proxy2, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
		require.Nil(t, err, "should be able to connect with target disabled")
		defer proxy2.Close()

		for i := 0; i < 10; i++ {
			err := proxy2.Query(fmt.Sprintf(
				"INSERT INTO %s.%s (id, name) VALUES (bb%06d-0000-0000-0000-000000000001, 'disabled_%d')",
				ks, outageTable, i, i)).Exec()
			require.Nil(t, err, "write %d should succeed with target disabled: %v", i, err)
		}

		// Verify data on origin
		var name string
		err = tempCcmSetup.Origin.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = bb000000-0000-0000-0000-000000000001", ks, outageTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "disabled_0", name)
	})

	// Step 6: Restart target
	t.Run("step6_restart_target", func(t *testing.T) {
		err := tempCcmSetup.Target.StartNode(0)
		require.Nil(t, err, "failed to start target node")
		t.Log("Target node restarted")

		// Wait for it to be ready
		time.Sleep(5 * time.Second)

		// Verify target is actually up by querying it directly
		require.Eventually(t, func() bool {
			testSession, err := utils.ConnectToCluster(
				tempCcmSetup.Target.GetInitialContactPoint(), "cassandra", "cassandra", 9042)
			if err != nil {
				return false
			}
			testSession.Close()
			return true
		}, 30*time.Second, 1*time.Second, "target did not come back up")
	})

	// Step 7: Re-enable target
	t.Run("step7_reenable_target", func(t *testing.T) {
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/target/enable", outageHTTPAddr), "application/json", nil)
		require.Nil(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		t.Log("Target re-enabled via API")
	})

	// Step 8: New connections should write to both clusters
	t.Run("step8_writes_to_both_after_reenable", func(t *testing.T) {
		// New connection to get a fresh ClientHandler with target connector
		proxy3, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
		require.Nil(t, err, "should be able to connect after target re-enabled")
		defer proxy3.Close()

		require.Nil(t, proxy3.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (cc000001-0000-0000-0000-000000000001, 'after_reenable')", ks, outageTable)).Exec())

		// Verify on target — need a fresh session since target was restarted
		targetSession, err := utils.ConnectToCluster(
			tempCcmSetup.Target.GetInitialContactPoint(), "cassandra", "cassandra", 9042)
		require.Nil(t, err, "should be able to connect to restarted target")
		defer targetSession.Close()

		var name string
		err = targetSession.Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = cc000001-0000-0000-0000-000000000001", ks, outageTable)).Scan(&name)
		require.Nil(t, err, "data should be on target after re-enable")
		require.Equal(t, "after_reenable", name)
	})
}

// TestTargetToggleBlockedByConfigCCM verifies that the toggle API rejects disable
// when incompatible configurations are active (DUAL_ASYNC_ON_SECONDARY or
// ForwardClientCredentialsToOrigin). These configs require target to be reachable
// and are only used in later migration phases.
func TestTargetToggleBlockedByConfigCCM(t *testing.T) {
	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	t.Run("blocked_by_dual_async", func(t *testing.T) {
		testConfig := setup.NewTestConfig(originCluster.GetInitialContactPoint(), targetCluster.GetInitialContactPoint())
		testConfig.ReadMode = config.ReadModeDualAsyncOnSecondary

		proxyInstance, err := setup.NewProxyInstanceWithConfig(testConfig)
		require.Nil(t, err)
		defer proxyInstance.Shutdown()

		// Attempt to disable — should be rejected
		err = proxyInstance.SetTargetEnabled(false)
		require.NotNil(t, err, "disable should be rejected with DUAL_ASYNC_ON_SECONDARY")
		require.Contains(t, err.Error(), "DUAL_ASYNC_ON_SECONDARY")
		require.True(t, proxyInstance.IsTargetEnabled(), "target should remain enabled")

		// Enable should always work
		err = proxyInstance.SetTargetEnabled(true)
		require.Nil(t, err, "enable should always succeed")
	})

	t.Run("blocked_by_forward_client_creds", func(t *testing.T) {
		testConfig := setup.NewTestConfig(originCluster.GetInitialContactPoint(), targetCluster.GetInitialContactPoint())
		testConfig.ForwardClientCredentialsToOrigin = true

		proxyInstance, err := setup.NewProxyInstanceWithConfig(testConfig)
		require.Nil(t, err)
		defer proxyInstance.Shutdown()

		// Attempt to disable — should be rejected
		err = proxyInstance.SetTargetEnabled(false)
		require.NotNil(t, err, "disable should be rejected with ForwardClientCredentialsToOrigin")
		require.Contains(t, err.Error(), "forward_client_credentials_to_origin")
		require.True(t, proxyInstance.IsTargetEnabled(), "target should remain enabled")
	})

	t.Run("allowed_with_default_config", func(t *testing.T) {
		testConfig := setup.NewTestConfig(originCluster.GetInitialContactPoint(), targetCluster.GetInitialContactPoint())

		proxyInstance, err := setup.NewProxyInstanceWithConfig(testConfig)
		require.Nil(t, err)
		defer proxyInstance.Shutdown()

		// Should succeed with default config
		err = proxyInstance.SetTargetEnabled(false)
		require.Nil(t, err, "disable should succeed with default config")
		require.False(t, proxyInstance.IsTargetEnabled())

		err = proxyInstance.SetTargetEnabled(true)
		require.Nil(t, err)
		require.True(t, proxyInstance.IsTargetEnabled())
	})

	t.Run("blocked_via_http_api", func(t *testing.T) {
		testConfig := setup.NewTestConfig(originCluster.GetInitialContactPoint(), targetCluster.GetInitialContactPoint())
		testConfig.ReadMode = config.ReadModeDualAsyncOnSecondary

		proxyInstance, err := setup.NewProxyInstanceWithConfig(testConfig)
		require.Nil(t, err)
		defer proxyInstance.Shutdown()

		// Start HTTP server on a unique port
		blockedHTTPAddr := "localhost:14096"
		mux := http.NewServeMux()
		mux.Handle("/api/v1/target", httpzdmproxy.TargetHandler(proxyInstance))
		mux.Handle("/api/v1/target/disable", httpzdmproxy.TargetHandler(proxyInstance))
		srv := &http.Server{Addr: blockedHTTPAddr, Handler: mux}
		go func() {
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				log.Warnf("blocked test http server error: %v", err)
			}
		}()
		defer srv.Close()

		require.Eventually(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/target", blockedHTTPAddr))
			if err != nil {
				return false
			}
			resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, 5*time.Second, 100*time.Millisecond, "HTTP server did not start")

		// POST disable — should get 409 Conflict
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/target/disable", blockedHTTPAddr), "application/json", nil)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusConflict, resp.StatusCode,
			"disable should return 409 Conflict when DUAL_ASYNC is enabled")

		body, err := io.ReadAll(resp.Body)
		require.Nil(t, err)
		var status targetStatusResponse
		require.Nil(t, json.Unmarshal(body, &status))
		require.True(t, status.Enabled, "should remain enabled")
		require.Contains(t, status.Message, "DUAL_ASYNC_ON_SECONDARY")
	})
}

// ================================================================
// Helper functions
// ================================================================

type targetStatusResponse struct {
	Enabled bool   `json:"enabled"`
	Message string `json:"message,omitempty"`
}

func gatherToggleMetricLines(t *testing.T) []string {
	t.Helper()
	statusCode, rspStr, err := utils.GetMetrics(toggleHTTPAddr)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)

	var result []string
	for _, line := range strings.Split(rspStr, "\n") {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			result = append(result, line)
		}
	}
	return result
}

func getMetricValue(t *testing.T, keyspace, table, cluster string) float64 {
	t.Helper()
	lines := gatherToggleMetricLines(t)
	prefix := fmt.Sprintf(`zdm_proxy_write_success_total{cluster="%s",keyspace="%s",table="%s"}`, cluster, keyspace, table)
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
				require.Nil(t, err, "failed to parse metric value from: %s", line)
				return val
			}
		}
	}
	t.Fatalf("metric not found: %s", prefix)
	return 0
}

func postTargetToggle(t *testing.T, action string) {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/target/%s", toggleHTTPAddr, action), "application/json", nil)
	require.Nil(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func requireTargetEnabledGauge(t *testing.T, lines []string, expectedValue float64) {
	t.Helper()
	for _, line := range lines {
		if strings.HasPrefix(line, "zdm_target_enabled") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
				require.Nil(t, err)
				require.Equal(t, expectedValue, val, "target_enabled gauge mismatch")
				return
			}
		}
	}
	t.Errorf("zdm_target_enabled gauge not found in metrics output")
}

func requireTargetStatus(t *testing.T, expectedEnabled bool) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/target", toggleHTTPAddr))
	require.Nil(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	var status struct {
		Enabled bool `json:"enabled"`
	}
	require.Nil(t, json.Unmarshal(body, &status))
	require.Equal(t, expectedEnabled, status.Enabled, "target enabled status mismatch")
}
