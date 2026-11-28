package adit

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// apiError is an S3 error with its HTTP status.
type apiError struct {
	Code    string
	Message string
	Status  int
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

func errf(status int, code, format string, args ...any) *apiError {
	return &apiError{Code: code, Message: fmt.Sprintf(format, args...), Status: status}
}

var (
	errAccessDenied        = errf(http.StatusForbidden, "AccessDenied", "Access Denied")
	errSignatureMismatch   = errf(http.StatusForbidden, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
	errInvalidAccessKey    = errf(http.StatusForbidden, "InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.")
	errTimeSkewed          = errf(http.StatusForbidden, "RequestTimeTooSkewed", "The difference between the request time and the current time is too large.")
	errExpired             = errf(http.StatusForbidden, "AccessDenied", "Request has expired")
	errNoSuchBucket        = errf(http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
	errNoSuchKey           = errf(http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
	errBucketOwned         = errf(http.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.")
	errBucketNotEmpty      = errf(http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty")
	errInvalidBucketName   = errf(http.StatusBadRequest, "InvalidBucketName", "The specified bucket is not valid.")
	errKeyTooLong          = errf(http.StatusBadRequest, "KeyTooLongError", "Your key is too long")
	errMissingLength       = errf(http.StatusLengthRequired, "MissingContentLength", "You must provide the Content-Length HTTP header.")
	errEntityTooLarge      = errf(http.StatusBadRequest, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size.")
	errIncompleteBody      = errf(http.StatusBadRequest, "IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.")
	errBadDigest           = errf(http.StatusBadRequest, "BadDigest", "The Content-MD5 or checksum value that you specified did not match what the server received.")
	errInvalidDigest       = errf(http.StatusBadRequest, "InvalidDigest", "The Content-MD5 or checksum value that you specified is not valid.")
	errContentSHA256       = errf(http.StatusBadRequest, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.")
	errInvalidRange        = errf(http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable")
	errPreconditionFailed  = errf(http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
	errMalformedXML        = errf(http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.")
	errMetadataTooLarge    = errf(http.StatusBadRequest, "MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.")
	errMethodNotAllowed    = errf(http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.")
	errNotImplemented      = errf(http.StatusNotImplemented, "NotImplemented", "A header or operation you provided implies functionality that is not implemented.")
	errServiceUnavailable  = errf(http.StatusServiceUnavailable, "ServiceUnavailable", "Not enough storage nodes are available. Please retry.")
	errQuotaExceeded       = errf(http.StatusForbidden, "QuotaExceeded", "The request would exceed the storage quota of this bucket or access key.")
	errInternal            = errf(http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.")
	errNoSuchVersion       = errf(http.StatusNotFound, "NoSuchVersion", "The specified version does not exist.")
	errNoSuchUpload        = errf(http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")
	errInvalidPart         = errf(http.StatusBadRequest, "InvalidPart", "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.")
	errInvalidPartOrder    = errf(http.StatusBadRequest, "InvalidPartOrder", "The list of parts was not in ascending order. The parts list must be specified in order by part number.")
	errEntityTooSmall      = errf(http.StatusBadRequest, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.")
	errInvalidContinuation = errf(http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect")
)

// S3Error is the XML error body returned on the S3 surface.
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}
