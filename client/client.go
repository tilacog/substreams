package client

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"

	"github.com/streamingfast/dgrpc"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	xdscreds "google.golang.org/grpc/credentials/xds"
	_ "google.golang.org/grpc/xds"
)

type SubstreamsClientConfig struct {
	endpoint  string
	jwt       string
	insecure  bool
	plaintext bool
}

func NewSubstreamsClientConfig(endpoint string, jwt string, insecure bool, plaintext bool) *SubstreamsClientConfig {
	return &SubstreamsClientConfig{
		endpoint:  endpoint,
		jwt:       jwt,
		insecure:  insecure,
		plaintext: plaintext,
	}
}

func NewSubstreamsClient(config *SubstreamsClientConfig) (cli pbsubstreams.StreamClient, closeFunc func() error, callOpts []grpc.CallOption, err error) {
	if config == nil {
		panic("substreams client config not set")
	}
	endpoint := config.endpoint
	jwt := config.jwt
	usePlainTextConnection := config.plaintext
	useInsecureTLSConnection := config.insecure

	zlog.Info("creating new client", zap.String("endpoint", endpoint), zap.Bool("jwt_present", jwt != ""), zap.Bool("plaintext", usePlainTextConnection), zap.Bool("insecure", useInsecureTLSConnection))

	bootStrapFilename := os.Getenv("GRPC_XDS_BOOTSTRAP")
	zlog.Info("looked for GRPC_XDS_BOOTSTRAP", zap.String("filename", bootStrapFilename))

	var dialOptions []grpc.DialOption
	skipAuth := jwt == "" || usePlainTextConnection
	if bootStrapFilename != "" {
		log.Println("Using xDS credentials...")
		creds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{FallbackCreds: insecure.NewCredentials()})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create xDS credentials: %v", err)
		}
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	} else {

		bootStrapFilename := os.Getenv("GRPC_XDS_BOOTSTRAP")
		zlog.Info("looked for GRPC_XDS_BOOTSTRAP", zap.String("filename", bootStrapFilename))

		var dialOptions []grpc.DialOption
		if bootStrapFilename != "" {
			log.Println("Using xDS credentials...")
			creds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{FallbackCreds: insecure.NewCredentials()})
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to create xDS credentials: %v", err)
			}
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
		} else {
			if useInsecureTLSConnection && usePlainTextConnection {
				return nil, nil, nil, fmt.Errorf("option --insecure and --plaintext are mutually exclusive, they cannot be both specified at the same time")
			}
			switch {
			case usePlainTextConnection:
				zlog.Debug("setting plain text option")

				dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

			case useInsecureTLSConnection:
				zlog.Debug("setting insecure tls connection option")
				dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}
			}
		}
	}

	dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()))
	dialOptions = append(dialOptions, grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()))

	zlog.Debug("getting connection", zap.String("endpoint", endpoint))
	conn, err := dgrpc.NewExternalClient(endpoint, dialOptions...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to create external gRPC client: %w", err)
	}
	closeFunc = conn.Close

	if !skipAuth {
		zlog.Debug("creating oauth access", zap.String("endpoint", endpoint))
		creds := oauth.NewOauthAccess(&oauth2.Token{AccessToken: jwt, TokenType: "Bearer"})
		callOpts = append(callOpts, grpc.PerRPCCredentials(creds))
	}

	zlog.Debug("creating new client", zap.String("endpoint", endpoint))
	cli = pbsubstreams.NewStreamClient(conn)
	zlog.Debug("client created")
	return
}
