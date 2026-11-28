package ledger

import (
	"context"
	"crypto/md5" // #nosec G501 -- S3 multipart ETag, not used for security
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrNoSuchUpload     = errors.New("ledger: no such upload")
	ErrInvalidPart      = errors.New("ledger: invalid part")
	ErrInvalidPartOrder = errors.New("ledger: parts not in ascending order")
	ErrEntityTooSmall   = errors.New("ledger: part smaller than the minimum allowed size")
)

// MinPartSize is the minimum size of every part except the last.
const MinPartSize = 5 << 20

// Upload is an in-progress multipart upload.
type Upload struct {
	UploadID          string
	Key               string
	Headers           map[string]string
	ChecksumAlgorithm string
	ChecksumType      string // COMPOSITE or FULL_OBJECT when ChecksumAlgorithm is set
	Tags              map[string]string
	AccessKey         string // who started the upload
	Initiated         time.Time
}

// PartChecksum is a stored part as seen by a checksum combiner.
type PartChecksum struct {
	Number   int
	Size     int64
	Checksum string // base64, as sent by the client for that part
}

// ChecksumFunc computes an upload's final checksum from its parts (in
// order), or returns an S3 error. It runs inside the Complete transaction.
type ChecksumFunc func(u Upload, parts []PartChecksum) (value string, err error)

// Part is an uploaded part.
type Part struct {
	Number        int
	Size          int64
	ETag          string
	ChecksumValue string
	LastModified  time.Time
	ChunkSize     int
	Nuggets       []NuggetRef
}

// CompletePart is a part named in a CompleteMultipartUpload request.
type CompletePart struct {
	Number int
	ETag   string
}

// CreateUpload starts a multipart upload. Only Headers, Tags,
// ChecksumAlgorithm and ChecksumType of u are used.
func (l *Ledger) CreateUpload(ctx context.Context, bucket, key string, u Upload) (string, error) {
	if u.Headers == nil {
		u.Headers = map[string]string{}
	}
	if u.Tags == nil {
		u.Tags = map[string]string{}
	}
	id := newID()
	tag, err := l.pool.Exec(ctx, `
		INSERT INTO multipart_uploads (upload_id, bucket_id, key, headers, checksum_algorithm, checksum_type, tags, access_key)
		SELECT $1, id, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7, $8 FROM buckets WHERE name = $2`,
		id, bucket, key, u.Headers, u.ChecksumAlgorithm, u.ChecksumType, u.Tags, u.AccessKey)
	if err == nil && tag.RowsAffected() == 0 {
		return "", ErrNoSuchBucket
	}
	return id, err
}

// lockUpload locks the upload row; exclusive blocks concurrent completes and aborts.
func lockUpload(ctx context.Context, tx pgx.Tx, bucket, key, uploadID string, exclusive bool) (Upload, int64, error) {
	lock := "FOR SHARE OF u"
	if exclusive {
		lock = "FOR UPDATE OF u"
	}
	u := Upload{UploadID: uploadID, Key: key}
	var bucketID int64
	var algo *string
	err := tx.QueryRow(ctx, `
		SELECT u.bucket_id, u.headers, u.checksum_algorithm, coalesce(u.checksum_type, ''), u.tags, u.access_key, u.created_at
		FROM multipart_uploads u JOIN buckets b ON b.id = u.bucket_id
		WHERE u.upload_id = $1 AND b.name = $2 AND u.key = $3 AND u.completed_at IS NULL `+lock, uploadID, bucket, key).
		Scan(&bucketID, &u.Headers, &algo, &u.ChecksumType, &u.Tags, &u.AccessKey, &u.Initiated)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM buckets WHERE name = $1)", bucket).Scan(&exists); err == nil && !exists {
			return u, 0, ErrNoSuchBucket
		}
		return u, 0, ErrNoSuchUpload
	}
	if algo != nil {
		u.ChecksumAlgorithm = *algo
	}
	return u, bucketID, err
}

// GetUpload returns an upload's metadata.
func (l *Ledger) GetUpload(ctx context.Context, bucket, key, uploadID string) (u Upload, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		u, _, err = lockUpload(ctx, tx, bucket, key, uploadID, false)
		return err
	})
	return u, err
}

// PutPart stores (or replaces) a part.
func (l *Ledger) PutPart(ctx context.Context, bucket, key, uploadID string, p Part) error {
	return pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		if _, _, err := lockUpload(ctx, tx, bucket, key, uploadID, false); err != nil {
			return err
		}
		manifestID, err := insertManifest(ctx, tx, p.ChunkSize, p.Nuggets)
		if err != nil {
			return err
		}
		var old *int64
		err = tx.QueryRow(ctx, "SELECT manifest_id FROM multipart_parts WHERE upload_id = $1 AND part_number = $2 FOR UPDATE",
			uploadID, p.Number).Scan(&old)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO multipart_parts (upload_id, part_number, manifest_id, size, etag, checksum_value)
			VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))
			ON CONFLICT (upload_id, part_number) DO UPDATE SET manifest_id = EXCLUDED.manifest_id, size = EXCLUDED.size,
				etag = EXCLUDED.etag, checksum_value = EXCLUDED.checksum_value, created_at = now()`,
			uploadID, p.Number, manifestID, p.Size, p.ETag, p.ChecksumValue); err != nil {
			return err
		}
		if old != nil {
			return dropManifest(ctx, tx, *old)
		}
		return nil
	})
}

// ListParts returns up to limit parts numbered above marker.
func (l *Ledger) ListParts(ctx context.Context, bucket, key, uploadID string, marker, limit int) (u Upload, parts []Part, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		if u, _, err = lockUpload(ctx, tx, bucket, key, uploadID, false); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT part_number, size, etag, coalesce(checksum_value, ''), created_at
			FROM multipart_parts WHERE upload_id = $1 AND part_number > $2 ORDER BY part_number LIMIT $3`, uploadID, marker, limit)
		if err != nil {
			return err
		}
		parts, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Part, error) {
			var p Part
			return p, r.Scan(&p.Number, &p.Size, &p.ETag, &p.ChecksumValue, &p.LastModified)
		})
		return err
	})
	return u, parts, err
}

func normETag(s string) string { return strings.Trim(strings.TrimSpace(s), `"`) }

// CompleteUpload assembles the named parts into a new object version. The
// object's manifest is the concatenation of the parts' manifests; the ETag is
// the MD5 of the parts' binary MD5s, suffixed with the part count.
//
// checksum, if non-nil, computes the object's checksum from the parts' checksums.
func (l *Ledger) CompleteUpload(ctx context.Context, bucket, key, uploadID string, list []CompletePart, cond Condition,
	checksum ChecksumFunc) (etag string, res WriteResult, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		u, bucketID, err := lockUpload(ctx, tx, bucket, key, uploadID, true)
		if errors.Is(err, ErrNoSuchUpload) {
			// A retried Complete of a finished upload returns the original result.
			var e, v, versioning *string
			qerr := tx.QueryRow(ctx, `SELECT u.completed_etag, u.completed_version, b.versioning
				FROM multipart_uploads u JOIN buckets b ON b.id = u.bucket_id
				WHERE u.upload_id = $1 AND b.name = $2 AND u.key = $3 AND u.completed_at IS NOT NULL`,
				uploadID, bucket, key).Scan(&e, &v, &versioning)
			if qerr == nil && e != nil {
				etag, res.VersionID, res.Versioning = *e, *v, *versioning
				return nil
			}
			return err
		}
		if err != nil {
			return err
		}
		if len(list) == 0 {
			return ErrInvalidPart
		}
		type stored struct {
			manifest int64
			size     int64
			etag     string
			checksum string
		}
		rows, err := tx.Query(ctx, `SELECT part_number, manifest_id, size, etag, coalesce(checksum_value, '')
			FROM multipart_parts WHERE upload_id = $1`, uploadID)
		if err != nil {
			return err
		}
		have := map[int]stored{}
		for rows.Next() {
			var n int
			var s stored
			if err := rows.Scan(&n, &s.manifest, &s.size, &s.etag, &s.checksum); err != nil {
				return err
			}
			have[n] = s
		}
		if err := rows.Err(); err != nil {
			return err
		}

		md5s := md5.New() // #nosec G401
		var nuggets []NuggetRef
		var size int64
		sizes := make([]int64, 0, len(list))
		sums := make([]PartChecksum, 0, len(list))
		for i := 1; i < len(list); i++ {
			if list[i].Number <= list[i-1].Number {
				return ErrInvalidPartOrder
			}
		}
		for i, cp := range list {
			s, ok := have[cp.Number]
			if !ok || normETag(cp.ETag) != normETag(s.etag) {
				return fmt.Errorf("%w: part %d", ErrInvalidPart, cp.Number)
			}
			if i < len(list)-1 && s.size < MinPartSize {
				return ErrEntityTooSmall
			}
			raw, err := hex.DecodeString(normETag(s.etag))
			if err != nil {
				return fmt.Errorf("ledger: part %d has malformed etag: %w", cp.Number, err)
			}
			md5s.Write(raw)
			refs, err := loadManifest(ctx, tx, s.manifest)
			if err != nil {
				return err
			}
			nuggets = append(nuggets, refs...)
			size += s.size
			sizes = append(sizes, s.size)
			sums = append(sums, PartChecksum{Number: cp.Number, Size: s.size, Checksum: s.checksum})
		}
		var sumValue string
		if checksum != nil && u.ChecksumAlgorithm != "" {
			if sumValue, err = checksum(u, sums); err != nil {
				return err
			}
			res.ChecksumAlgorithm, res.ChecksumValue, res.ChecksumType = u.ChecksumAlgorithm, sumValue, u.ChecksumType
		}
		etag = fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(md5s.Sum(nil)), len(list))

		// References move from the part manifests to the object manifest.
		manifestID, err := insertManifest(ctx, tx, 0, nuggets)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM multipart_parts WHERE upload_id = $1", uploadID); err != nil {
			return err
		}
		for _, s := range have {
			if err := dropManifest(ctx, tx, s.manifest); err != nil {
				return err
			}
		}

		var versioning string
		if err := tx.QueryRow(ctx, "SELECT versioning FROM buckets WHERE id = $1 FOR SHARE", bucketID).Scan(&versioning); err != nil {
			return err
		}
		res.Versioning = versioning
		objectID, err := lockObject(ctx, tx, bucketID, key, true)
		if err != nil {
			return err
		}
		if err := checkCondition(ctx, tx, objectID, cond); err != nil {
			return err
		}
		res.VersionID, err = addVersion(ctx, tx, objectID, versioning, &manifestID, Version{
			Size: size, ETag: etag, Headers: u.Headers, Tags: u.Tags, PartsCount: len(list), PartSizes: sizes, AccessKey: u.AccessKey,
			ChecksumAlgorithm: res.ChecksumAlgorithm, ChecksumValue: sumValue, ChecksumType: res.ChecksumType,
		})
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE multipart_uploads SET completed_etag = $2, completed_version = $3, completed_at = now()
			WHERE upload_id = $1`, uploadID, etag, res.VersionID)
		return err
	})
	return etag, res, err
}

// AbortUpload discards an upload and its parts.
func (l *Ledger) AbortUpload(ctx context.Context, bucket, key, uploadID string) error {
	return pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		if _, _, err := lockUpload(ctx, tx, bucket, key, uploadID, true); err != nil {
			return err
		}
		return abortLocked(ctx, tx, uploadID)
	})
}

func abortLocked(ctx context.Context, tx pgx.Tx, uploadID string) error {
	rows, err := tx.Query(ctx, "SELECT manifest_id FROM multipart_parts WHERE upload_id = $1", uploadID)
	if err != nil {
		return err
	}
	manifests, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM multipart_uploads WHERE upload_id = $1", uploadID); err != nil {
		return err
	}
	for _, m := range manifests {
		if err := dropManifest(ctx, tx, m); err != nil {
			return err
		}
	}
	return nil
}

// ListUploads lists in-progress uploads ordered by key then upload ID,
// starting after (keyMarker, uploadMarker).
func (l *Ledger) ListUploads(ctx context.Context, bucket, prefix, keyMarker, uploadMarker string, limit int) ([]Upload, error) {
	if _, err := l.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	rows, err := l.pool.Query(ctx, `
		SELECT u.upload_id, u.key, u.created_at
		FROM multipart_uploads u JOIN buckets b ON b.id = u.bucket_id
		WHERE b.name = $1 AND starts_with(u.key, $2) AND u.completed_at IS NULL
		  AND (u.key > $3 OR (u.key = $3 AND $4 <> '' AND u.upload_id > $4))
		ORDER BY u.key, u.upload_id
		LIMIT $5`, bucket, prefix, keyMarker, uploadMarker, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Upload, error) {
		var u Upload
		return u, r.Scan(&u.UploadID, &u.Key, &u.Initiated)
	})
}
