from apixis.core.utils.snow import options, generator


options = options.IdGeneratorOptions(worker_id=23)
idgen = generator.DefaultIdGenerator()
idgen.set_id_generator(options)

# Use ```uid = idgen.next_id()``` to generate an ID unique within this process.
