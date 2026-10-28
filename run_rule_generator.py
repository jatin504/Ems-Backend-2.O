"""Start the approved rule generator without starting the EMS backend."""
from vmalert_rule_generator.generate_vmalert_rules import main


if __name__ == "__main__":
    # Periodically regenerate changed rules; vmalert itself evaluates the YAML.
    main(service_default=True)
