package ranking

import (
	"log"
	"math"
	"sort"
	"time"

	"fayan/internal/models"
	"fayan/internal/repository"
)

type scoreWithID struct {
	id    int32
	score float64
}

// inLink represents a single weighted in-edge in the adjacency list.
// Weight is 1.0 for follow edges and vouchWeight for vouch-only edges.
type inLink struct {
	source int32
	weight float64
}

// Calculator handles PageRank and TrustRank calculations
type Calculator struct {
	repo            *repository.Repository
	seedPubkeys     []string
	trustRankWeight float64
	pageRankWeight  float64
	vouchWeight     float64
}

// NewCalculator creates a new Calculator instance.
// vouchWeight is the relative weight of a vouch-only edge (follow edges are 1.0).
func NewCalculator(repo *repository.Repository, seedPubkeys []string, trustRankWeight, pageRankWeight, vouchWeight float64) *Calculator {
	return &Calculator{
		repo:            repo,
		seedPubkeys:     seedPubkeys,
		trustRankWeight: trustRankWeight,
		pageRankWeight:  pageRankWeight,
		vouchWeight:     vouchWeight,
	}
}

// Calculate performs both PageRank and TrustRank algorithms on the stored connections and updates scores.
func (c *Calculator) Calculate() error {
	pubkeyToID := make(map[string]int32)
	idToPubkey := make([]string, 0)

	type edge struct {
		source int32
		target int32
		weight float64
	}
	edges := make([]edge, 0, 1000)

	getID := func(pubkey string) int32 {
		if id, ok := pubkeyToID[pubkey]; ok {
			return id
		}
		id := int32(len(idToPubkey))
		pubkeyToID[pubkey] = id
		idToPubkey = append(idToPubkey, pubkey)
		return id
	}

	log.Println("   [INFO] Streaming connections from database...")
	connectionCount := 0

	cutoffTime := time.Now().UTC().AddDate(0, 0, -30)

	// edgeSet dedupes (source, target) pairs across follow and vouch edges so
	// a user who both follows and vouches for the same target contributes a
	// single follow-weighted edge (not double flow).
	edgeSet := make(map[int64]bool)
	encodeEdge := func(s, t int32) int64 { return int64(s)<<32 | int64(uint32(t)) }

	err := c.repo.StreamConnectionsInTx(func(conn models.Connection) error {
		sourceID := getID(conn.Source)
		targetID := getID(conn.Target)

		if sourceID != targetID {
			key := encodeEdge(sourceID, targetID)
			if !edgeSet[key] {
				edgeSet[key] = true
				edges = append(edges, edge{source: sourceID, target: targetID, weight: 1.0})
			}
		}
		connectionCount++
		return nil
	}, &cutoffTime)

	if err != nil {
		return err
	}

	// Vouch edges: admit only those from pubkeys with positive last-round
	// TrustRank (seeds always admitted so the feature works on first run when
	// no one has trust_score written yet). Skipped entirely when the feature
	// is disabled (vouchWeight <= 0).
	vouchAdmitted := 0
	if c.vouchWeight > 0 {
		qualifying, qErr := c.repo.GetPubkeysWithPositiveTrust()
		if qErr != nil {
			log.Printf("   [WARN] Failed to load qualifying pubkeys for vouch admission: %v", qErr)
			qualifying = make(map[string]struct{})
		}
		for _, s := range c.seedPubkeys {
			qualifying[s] = struct{}{}
		}
		if err := c.repo.StreamVouches(func(v models.Vouch) error {
			if _, ok := qualifying[v.Source]; !ok {
				return nil
			}
			sourceID := getID(v.Source)
			targetID := getID(v.Target)
			if sourceID == targetID {
				return nil
			}
			key := encodeEdge(sourceID, targetID)
			if edgeSet[key] {
				return nil
			}
			edgeSet[key] = true
			edges = append(edges, edge{source: sourceID, target: targetID, weight: c.vouchWeight})
			vouchAdmitted++
			return nil
		}, &cutoffTime); err != nil {
			return err
		}
		log.Printf("   [INFO] Vouch edges admitted: %d (weight=%.2f)", vouchAdmitted, c.vouchWeight)
	}

	numNodes := len(idToPubkey)
	if numNodes == 0 {
		log.Println("   [WARN] Graph is empty, skipping calculation")
		return nil
	}

	log.Printf("   [INFO] Processing %d nodes, %d connections (+ %d vouches)", numNodes, connectionCount, vouchAdmitted)

	// Build seed node set for TrustRank
	seedSet := make(map[int32]bool)
	for _, pubkey := range c.seedPubkeys {
		if id, ok := pubkeyToID[pubkey]; ok {
			seedSet[id] = true
		}
	}
	log.Printf("   [INFO] Found %d seed nodes in graph (out of %d configured)", len(seedSet), len(c.seedPubkeys))

	// Build the weighted graph.
	// outWeight[i] is the sum of outgoing edge weights (used by flow math).
	// outDegree[i] is the discrete count (used only for the Following field).
	inLinks := make([][]inLink, numNodes)
	outWeight := make([]float64, numNodes)
	outDegree := make([]int32, numNodes)

	for _, e := range edges {
		inLinks[e.target] = append(inLinks[e.target], inLink{source: e.source, weight: e.weight})
		outWeight[e.source] += e.weight
		outDegree[e.source]++
	}

	edges = nil // Release memory

	dampingFactor := 0.85
	tolerance := 1e-5
	maxIterations := 100

	// Run PageRank
	log.Println("   [INFO] Running PageRank...")
	pageScores := c.runPageRank(numNodes, inLinks, outWeight, dampingFactor, tolerance, maxIterations)

	// Run TrustRank
	var trustScores []float64
	if len(seedSet) > 0 {
		log.Println("   [INFO] Running TrustRank...")
		trustScores = c.runTrustRank(numNodes, inLinks, outWeight, seedSet, dampingFactor, tolerance, maxIterations)
	} else {
		log.Println("   [WARN] No seed nodes found, skipping TrustRank")
		trustScores = make([]float64, numNodes)
	}

	// Calculate combined scores
	scores := make([]float64, numNodes)
	for i := range numNodes {
		scores[i] = c.trustRankWeight*trustScores[i] + c.pageRankWeight*pageScores[i]
	}

	// Apply trust-weighted report penalty: scale each target's scores by
	// (1 - penalty) where penalty = R / (R + F + ε). R is the sum of reporter
	// trust_scores (only reporters with trust_score > 0 count); F is the sum
	// of follower/voucher trust_scores weighted by their edge weights. Rank
	// is computed after penalty so penalized accounts drop in the ordering.
	if reports, err := c.repo.GetTrustWeightedReports(); err != nil {
		log.Printf("   [WARN] Failed to load reports for penalty: %v", err)
	} else if len(reports) > 0 {
		fTrust := make([]float64, numNodes)
		for i := range numNodes {
			for _, link := range inLinks[i] {
				fTrust[i] += trustScores[link.source] * link.weight
			}
		}
		penalized := 0
		for i := range numNodes {
			agg, ok := reports[idToPubkey[i]]
			if !ok || agg.TotalReporterTrust <= 0 {
				continue
			}
			penalty := agg.TotalReporterTrust / (agg.TotalReporterTrust + fTrust[i] + 1e-9)
			if penalty > 1 {
				penalty = 1
			}
			factor := 1 - penalty
			scores[i] *= factor
			trustScores[i] *= factor
			pageScores[i] *= factor
			penalized++
		}
		log.Printf("   [INFO] Applied report penalty to %d pubkeys", penalized)
	}

	// Calculate ranks based on scores
	rankList := make([]scoreWithID, numNodes)
	for i := range numNodes {
		rankList[i] = scoreWithID{id: int32(i), score: scores[i]}
	}

	sort.Slice(rankList, func(i, j int) bool {
		return rankList[i].score > rankList[j].score
	})

	// Update database using batch updates to reduce WAL growth
	log.Printf("   [INFO] Updating %d scores in database...", numNodes)

	const batchSize = 1000
	updates := make([]repository.PubkeyUpdate, 0, batchSize)
	updatedCount := 0

	for rank, item := range rankList {
		pubkey := idToPubkey[item.id]
		if pubkey == "" {
			continue
		}

		updates = append(updates, repository.PubkeyUpdate{
			Pubkey:     pubkey,
			Score:      scores[item.id],
			Rank:       rank + 1,
			TrustScore: trustScores[item.id],
			PageScore:  pageScores[item.id],
			Followers:  len(inLinks[item.id]),
			Following:  outDegree[item.id],
		})

		// Batch update when we reach batchSize
		if len(updates) >= batchSize {
			if err := c.repo.BatchUpdatePubkeys(updates); err != nil {
				log.Printf("   [WARN] Batch update failed: %v", err)
			} else {
				updatedCount += len(updates)
			}
			updates = updates[:0]
		}
	}

	// Update remaining items
	if len(updates) > 0 {
		if err := c.repo.BatchUpdatePubkeys(updates); err != nil {
			log.Printf("   [WARN] Final batch update failed: %v", err)
		} else {
			updatedCount += len(updates)
		}
	}

	log.Printf("   [INFO] Updated %d/%d pubkeys", updatedCount, numNodes)

	// Force WAL checkpoint after large batch operation
	log.Println("   [INFO] Running WAL checkpoint...")
	c.repo.Checkpoint()

	return nil
}

// runPageRank executes the weighted PageRank algorithm.
// Each in-edge carries its own weight; a node's score is distributed among
// its out-neighbors in proportion to edge weight (sum equals outWeight[j]).
func (c *Calculator) runPageRank(numNodes int, inLinks [][]inLink, outWeight []float64, damping, tolerance float64, maxIterations int) []float64 {
	scores := make([]float64, numNodes)
	newScores := make([]float64, numNodes)
	initialScore := 1.0 / float64(numNodes)

	for i := range scores {
		scores[i] = initialScore
	}

	for iter := 0; iter < maxIterations; iter++ {
		danglingSum := 0.0
		for i := range numNodes {
			if outWeight[i] == 0 {
				danglingSum += scores[i]
			}
		}

		for i := range numNodes {
			sum := 0.0
			for _, link := range inLinks[i] {
				sum += scores[link.source] * link.weight / outWeight[link.source]
			}
			newScores[i] = (1-damping)/float64(numNodes) + damping*(sum+danglingSum/float64(numNodes))
		}

		// Check convergence
		diff := 0.0
		for i := range numNodes {
			diff += math.Abs(newScores[i] - scores[i])
		}

		scores, newScores = newScores, scores

		if diff < tolerance {
			log.Printf("   [INFO] PageRank converged after %d iterations", iter+1)
			break
		}
	}

	return scores
}

// runTrustRank executes the weighted TrustRank algorithm.
func (c *Calculator) runTrustRank(numNodes int, inLinks [][]inLink, outWeight []float64, seedSet map[int32]bool, damping, tolerance float64, maxIterations int) []float64 {
	scores := make([]float64, numNodes)
	newScores := make([]float64, numNodes)

	// Initialize seed nodes with equal trust
	seedScore := 1.0 / float64(len(seedSet))
	for seedID := range seedSet {
		scores[seedID] = seedScore
	}

	for iter := range maxIterations {
		danglingSum := 0.0
		for i := range numNodes {
			if outWeight[i] == 0 {
				danglingSum += scores[i]
			}
		}

		for i := range numNodes {
			sum := 0.0
			for _, link := range inLinks[i] {
				sum += scores[link.source] * link.weight / outWeight[link.source]
			}

			// In TrustRank, dangling node scores only flow back to seed nodes
			if seedSet[int32(i)] {
				newScores[i] = (1-damping)*seedScore + damping*(sum+danglingSum*seedScore)
			} else {
				newScores[i] = damping * sum
			}
		}

		diff := 0.0
		for i := range numNodes {
			diff += math.Abs(newScores[i] - scores[i])
		}

		scores, newScores = newScores, scores

		if diff < tolerance {
			log.Printf("   [INFO] TrustRank converged after %d iterations", iter+1)
			break
		}
	}

	return scores
}
