package bwe

import (
	"errors"
	"net"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/cc"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/webrtc/v4"

	"sfu-lab/internal/netem"
)

const (
	bweInitial = 500_000
	bweMin     = 100_000
	bweMax     = 5_000_000
)

type Rig struct {
	API       *webrtc.API
	Estimator <-chan cc.BandwidthEstimator
	Shaper    *netem.ShapedConn
	Mux       ice.UDPMux
}

func NewRig() (*Rig, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	ir := &interceptor.Registry{}
	controller, err := cc.NewInterceptor(func() (cc.BandwidthEstimator, error) {
		return gcc.NewSendSideBWE(
			gcc.SendSideBWEInitialBitrate(bweInitial),
			gcc.SendSideBWEMinBitrate(bweMin),
			gcc.SendSideBWEMaxBitrate(bweMax),
		)
	})
	if err != nil {
		return nil, err
	}
	ch := make(chan cc.BandwidthEstimator, 1)
	controller.OnNewPeerConnection(func(_ string, e cc.BandwidthEstimator) {
		select {
		case ch <- e:
		default:
		}
	})
	ir.Add(controller)
	if err := webrtc.ConfigureTWCCHeaderExtensionSender(m, ir); err != nil {
		return nil, err
	}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		return nil, err
	}
	raw, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}
	shaper := netem.NewShapedConn(raw)
	mux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: shaper})
	se := webrtc.SettingEngine{}
	se.SetICEUDPMux(mux)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(ir), webrtc.WithSettingEngine(se))
	return &Rig{API: api, Estimator: ch, Shaper: shaper, Mux: mux}, nil
}

func TakeEstimator(ch <-chan cc.BandwidthEstimator) (cc.BandwidthEstimator, error) {
	select {
	case e := <-ch:
		return e, nil
	case <-time.After(time.Second):
		return nil, errors.New("bandwidth estimator was not created")
	}
}
