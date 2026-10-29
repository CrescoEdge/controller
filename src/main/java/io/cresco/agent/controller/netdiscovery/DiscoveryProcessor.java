package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.security.cert.Certificate;

public class DiscoveryProcessor {

	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;

	private String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
	private String validateMessage = "DISCOVERY_MESSAGE_VALIDATED";

	private DiscoveryCrypto discoveryCrypto;

	public DiscoveryProcessor(ControllerEngine controllerEngine) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(DiscoveryProcessor.class.getName(), CLogger.Level.Info);
		this.discoveryCrypto = new DiscoveryCrypto(controllerEngine);
	}

	private String getDiscoverySecret(DiscoveryNode discoveryNode) {
		String discoverySecret = null;
		try {

			if (discoveryNode.discovery_type == DiscoveryType.AGENT) {

				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");

			} else if (discoveryNode.discovery_type == DiscoveryType.REGION) {

				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");

			} else if (discoveryNode.discovery_type == DiscoveryType.GLOBAL) {

				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");

			} else {
				logger.error("processDiscoveryNode UNKNOWN DISCOVERY TYPE");
			}

		} catch (Exception ex) {
			logger.error("getDiscoverySecret() " + ex.getMessage());
		}
		return discoverySecret;
	}

	// W-GFS-5: advertise the port THIS controller's broker is bound to (dynamic port included), so a
	// peer bridges to this broker and not to whatever happens to listen on discovery_port_remote on
	// this host. On one host with several regional brokers that was the global's broker: the bridge
	// came up against the wrong broker, the identity check failed, the path was declared lost and
	// re-discovered every ~7 s ("Controller Path Lost" flapping).
	private int advertisedBrokerPort() {
		try {
			if (controllerEngine.getBroker() != null) {
				return controllerEngine.getBroker().getBoundBrokerPort();
			}
		} catch (Exception ignore) { }
		return plugin.getConfig().getIntegerParam("broker_port", 32010);
	}

	public DiscoveryNode processIncomingBroadCastDiscovery(DiscoveryNode discoveryNode, String localAddress, int localPort, String remoteAddress, int remotePort) {
		try {

			if (discoveryNode.discovery_type == DiscoveryType.NETWORK) {
				if(discoveryNode.nodeType == DiscoveryNode.NodeType.BROADCAST) {
					discoveryNode.setBroadcastResponse(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(), plugin.getAgent());
				} else {
					logger.error("processIncomingDiscoveryNode() discoveryNode.nodeType != DiscoveryNode.NodeType.BROADCAST");
				}
			} else {
				if(controllerEngine.cstate.isRegionalController()) {

					String discoverySecret = getDiscoverySecret(discoveryNode);

					if(discoverySecret != null) {

						String decryptedString = discoveryCrypto.decrypt(discoveryNode.broadcast_validator, discoverySecret);
						if (decryptedString != null) {
							if (decryptedString.equals(verifyMessage)) {

								if(discoveryNode.nodeType == DiscoveryNode.NodeType.DISCOVER) {
									discoveryNode.setDiscovered(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(),plugin.getAgent(),controllerEngine.reachableAgents().size(), discoveryCrypto.encrypt(validateMessage,discoverySecret));
									discoveryNode.discovered_broker_port = advertisedBrokerPort();
								} else if(discoveryNode.nodeType == DiscoveryNode.NodeType.CERTIFY) {

									String discovered_cert = configureCertTrust(discoveryNode);
									discoveryNode.setCertified(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(),plugin.getAgent(),controllerEngine.reachableAgents().size(), discoveryCrypto.encrypt(validateMessage,discoverySecret), discovered_cert);
									discoveryNode.discovered_broker_port = advertisedBrokerPort();

								} else {
									logger.error("processIncomingDiscoveryNode() discoveryNode.nodeType: " + discoveryNode.nodeType.name() + " !UNKNOWN!");
								}
							}
						}
					} else {
						logger.error("No secret found for " + discoveryNode.discovery_type.name() + " type");
						discoveryNode = null;
					}

				} else {
					logger.error("processDiscoveryNode() Discovery ERROR AGENT IN REGION SPACE");
					discoveryNode = null;
				}
			}

		} catch (Exception ex) {
			logger.error("processIncomingDiscoveryNode() " + ex.getMessage());
			discoveryNode = null;
		}
		return discoveryNode;
	}

	public DiscoveryNode generateBroadCastDiscovery(DiscoveryType disType, boolean sendCert) {

		DiscoveryNode discoveryNode = null;

		try {

			discoveryNode = new DiscoveryNode(disType);

			if (disType == DiscoveryType.AGENT) {

				String discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
				if (discoverySecret != null) {
					if(sendCert) {
						ownLeafForDiscovery();
						//sme.setParam("public_cert", controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()));

						discoveryNode.setCertify(discoveryCrypto.encrypt(verifyMessage, discoverySecret), controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()), plugin.getRegion(), plugin.getAgent());
					} else {
						discoveryNode.setDiscover(discoveryCrypto.encrypt(verifyMessage, discoverySecret));
					}
				} else {
					logger.error("discover() Error: discovery_secret_agent = null, no agent discovery");
				}

			} else if (disType == DiscoveryType.REGION) {

				String discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
				if (discoverySecret != null) {
					if(sendCert) {
						ownLeafForDiscovery();
						discoveryNode.setCertify(discoveryCrypto.encrypt(verifyMessage, discoverySecret),controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()), plugin.getRegion(), plugin.getAgent());
					} else {
						discoveryNode.setDiscover(discoveryCrypto.encrypt(verifyMessage, discoverySecret));
					}
				} else {
					logger.error("discover() Error: discovery_secret_region = null, no region discovery");
				}

			} else if (disType == DiscoveryType.GLOBAL) {
				String discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
				if (discoverySecret != null) {
					if(sendCert) {
						ownLeafForDiscovery();
						discoveryNode.setCertify(discoveryCrypto.encrypt(verifyMessage, discoverySecret),controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()), plugin.getRegion(), plugin.getAgent());
					} else {
						discoveryNode.setDiscover(discoveryCrypto.encrypt(verifyMessage, discoverySecret));
					}
				} else {
					logger.error("discover() Error: discovery_secret_global = null, , no global discovery");
				}

			} else if (disType == DiscoveryType.NETWORK) {

				discoveryNode = new DiscoveryNode(disType);

			} else {
				logger.error("generateDiscovery() Discovery Type UNKNOWN");
			}
		} catch (Exception ex) {
			logger.error("generateDiscovery() Error: " + ex.getMessage());
		}
		return discoveryNode;
	}

	public boolean  isValidatedAuthenication(DiscoveryNode discoveryNode) {
		boolean isValidated = false;
		try {

			String discoverySecret = null;
			if (discoveryNode.discovery_type == DiscoveryType.AGENT) {
				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
			} else if (discoveryNode.discovery_type == DiscoveryType.REGION) {
				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
			} else if (discoveryNode.discovery_type == DiscoveryType.GLOBAL) {
				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
			}

			if(discoverySecret != null) {
				String validatedString = discoveryCrypto.decrypt(discoveryNode.discovered_validator, discoverySecret);
				if (validatedString != null) {
					if (validatedString.equals(validateMessage)) {
						isValidated = true;
					} else {
						logger.error("isValidatedAuthenication() Error : validatedString != validateMessage");
					}
				} else {
					logger.error("isValidatedAuthenication() Error : validatedString == null ");
				}
			} else {
				logger.error("isValidatedAuthenication() Error : discoverySecret == null");
			}
		}
		catch(Exception ex) {
			logger.error(ex.getMessage());
		}

		return isValidated;
	}

	/** Make our leaf name our current identity before it goes out in a discovery exchange (legacy trust only). */
	private void ownLeafForDiscovery() {
		try {
			controllerEngine.getCertificateManager().ensureLeafIdentity();
		} catch (Exception ex) {
			logger.error("ownLeafForDiscovery " + ex.getMessage());
		}
	}

	/**
	 * Client side of a certified discovery exchange: trust the responder (or install the identity its region CA issued
	 * us), and record what binds later connections to it (PeerIdentity): in legacy per-node trust the responder's leaf
	 * key is pinned to the region/agent it reported; a REGION/GLOBAL responder is a controller allowed to bridge to us.
	 */
	public boolean setCertTrust(DiscoveryNode discoveryNode) {
		boolean isSet = setCertTrust(discoveryNode.getDiscoveredPath(), discoveryNode.discovered_cert);
		if (isSet) {
			learnPeer(discoveryNode.discovery_type, discoveryNode.discovered_region, discoveryNode.discovered_agent,
					discoveryNode.discovered_cert, !plugin.getConfig().getBooleanParam("security_regional_ca", false));
		}
		return isSet;
	}

	private void learnPeer(DiscoveryType type, String region, String agent, String certJson, boolean pinLeaf) {
		try {
			io.cresco.agent.controller.communication.PeerTrustRegistry reg = controllerEngine.getCertificateManager().getPeerTrust();
			if (pinLeaf && certJson != null) {
				java.security.cert.X509Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(certJson);
				if (certs != null && certs.length > 0 && certs[0] != null) {
					String why = reg.pin(region, agent, certs[0]);
					if (why != null) {
						logger.warn("Discovery: not pinning a key for " + region + "_" + agent + ": " + why);
					}
				}
			}
			if (type == DiscoveryType.REGION || type == DiscoveryType.GLOBAL) {
				reg.allowBridgePeer(region, agent);
			}
		} catch (Exception ex) {
			logger.error("learnPeer " + ex.getMessage());
		}
	}

	public boolean setCertTrust(String remoteAgentPath, String remoteCertString) {
		boolean isSet = false;
		try {
			if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) {
				// Regional-CA (Option C): the response carries THIS node's region-signed identity —
				// install it (swap our cert chain, trust the issuing CA that rides in the chain).
				isSet = controllerEngine.getCertificateManager().installEnrollment(remoteCertString);
			} else {
				Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
				controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath, certs);
				isSet = true;
			}
		} catch(Exception ex) {
			logger.error("setCertTrust Error " + ex.getMessage());
		}
		return isSet;
	}

	/** Responder side of a certified discovery exchange (see {@link #setCertTrust(DiscoveryNode)}). */
	private String configureCertTrust(DiscoveryNode discoveryNode) {
		boolean regionalCa = plugin.getConfig().getBooleanParam("security_regional_ca", false);
		String local = configureCertTrust(discoveryNode.getDiscoverPath(), discoveryNode.discover_cert, discoveryNode.discovery_type);
		if (local != null) {
			learnPeer(discoveryNode.discovery_type, discoveryNode.discover_region, discoveryNode.discover_agent,
					discoveryNode.discover_cert, !regionalCa);
		}
		return local;
	}

	private String configureCertTrust(String remoteAgentPath, String remoteCertString, DiscoveryType joinType) {
		String localCertString = null;
		try {
			if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) {
				// Regional-CA (Option C): act as the issuing authority — sign the joiner a leaf under our
				// region CA and return the chain for it to install. We trust the joiner via our own CA
				// (no per-node truststore entry needed), so trust material stays O(regions), not O(nodes^2).
				localCertString = controllerEngine.getCertificateManager().issueEnrollment(remoteCertString, joinType);
				if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager();
			} else {
				Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
				controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath, certs);
				if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager();
				localCertString = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
			}
		} catch(Exception ex) {
			logger.error("configureCertTrust Error " + ex.getMessage());
		}
		return localCertString;
	}
}
